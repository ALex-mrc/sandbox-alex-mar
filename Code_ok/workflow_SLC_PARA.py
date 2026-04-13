"""
workflow_SLC_PARA.py — Traitement parallèle de paires SLC Sentinel-1

Architecture :
  1. Nettoyage des locks pyrosar orphelins (anti-blocage)
  2. Préchauffage du cache XML des nœuds SNAP en série (anti-contention)
  3. Construction de tous les graphes XML en série (anti-lock)
  4. Exécution des jobs GPT en parallèle (ThreadPool, retry)
  5. Chargement des TIF résultants en xarray.Dataset

Dépendances optionnelles :
  - rioxarray : pip install rioxarray  (recommandé pour xarray géospatial complet)
  - psutil    : pip install psutil     (recommandé pour la vérification de process)
"""

from __future__ import annotations

import concurrent.futures
import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import xarray as xr
from pyroSAR.snap.auxil import gpt, parse_recipe, parse_node
from tqdm import tqdm

try:
    import rioxarray  # noqa: F401 — active le .rio accessor sur xarray
    _HAS_RIOXARRAY = True
except ImportError:
    _HAS_RIOXARRAY = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# CONFIGURATION — modifier ici pour lancer des traitements
# ──────────────────────────────────────────────────────────────

# Liste des paires à traiter. Chaque entrée = un job.
PAIRS: list[dict] = [
    {
        'input_1': Path(r'c:\Users\alexa\Downloads\S1A_IW_SLC__1SDV_20230818T154906_20230818T154932_049932_0601A0_FDD8.SAFE.zip'),
        'input_2': Path(r'c:\Users\alexa\Downloads\S1A_IW_SLC__1SDV_20231029T154907_20231029T154934_050982_062582_3B98.SAFE.zip'),
        'subswath': 'IW3',
        'polarisation_mode': 'VH',   # 'VV', 'VH' ou 'BOTH'
        'first_burst_index': 3,
        'last_burst_index': 7,
    },
    # Ajouter d'autres paires ici :
    # {
    #     'input_1': Path(r'c:\...\produit_A.SAFE.zip'),
    #     'input_2': Path(r'c:\...\produit_B.SAFE.zip'),
    #     'subswath': 'IW2',
    #     'polarisation_mode': 'VV',
    #     'first_burst_index': 1,
    #     'last_burst_index': 5,
    # },
]

OUTPUT_DIR            = Path(r'c:\Users\alexa\Downloads')
OUTPUT_CRS            = 'EPSG:32631'
MASK_OUT_NO_ELEVATION = True

GPT_THREADS     = 4   # threads internes SNAP par job (paramètre -q)
GPT_WORKERS     = 2   # jobs GPT simultanés — commencer à 2, monter prudemment
GPT_MAX_RETRIES = 1   # nombre de réessais en cas d'erreur GPT

# Opérateurs dont le cache XML doit être préchauffé au démarrage
_OPERATORS = [
    'Read',
    'TOPSAR-Split',
    'Apply-Orbit-File',
    'Back-Geocoding',
    'Enhanced-Spectral-Diversity',
    'TOPSAR-Deburst',
    'Multilook',
    'Terrain-Correction',
    'Write',
]


# ──────────────────────────────────────────────────────────────
# TYPES
# ──────────────────────────────────────────────────────────────

@dataclass
class JobResult:
    pair_id:  str
    pol:      str
    tif_path: Path
    success:  bool
    error:    str = ''
    dataset:  Optional[xr.Dataset] = field(default=None, repr=False)


# ──────────────────────────────────────────────────────────────
# SÉCURITÉ ANTI-LOCK
# ──────────────────────────────────────────────────────────────

def _node_cache_dir() -> Path:
    """Retourne le répertoire de cache des nœuds pyrosar pour la version SNAP active."""
    from pyroSAR.examine import ExamineSnap
    version = ExamineSnap().get_version('microwavetbx')
    return Path.home() / '.pyrosar' / 'snap' / 'nodes' / version


def _clean_stale_locks() -> None:
    """
    Supprime les fichiers .lock orphelins du cache pyrosar.
    Si psutil est disponible, vérifie d'abord qu'aucun process SNAP n'est actif.
    """
    try:
        import psutil
        active = any(
            p.name().lower() in ('gpt.exe', 'snap64.exe', 'snap.exe', 'java.exe')
            for p in psutil.process_iter(['name'])
        )
        if active:
            log.warning('Process SNAP/GPT actif détecté — nettoyage des locks ignoré.')
            return
    except ImportError:
        log.info('psutil non disponible — nettoyage sans vérification de process actif.')

    cache_dir = _node_cache_dir()
    if not cache_dir.exists():
        return

    locks = list(cache_dir.glob('*.lock'))
    for lock in locks:
        try:
            lock.unlink()
            log.info(f'Lock orphelin supprimé : {lock.name}')
        except OSError as exc:
            log.warning(f'Impossible de supprimer {lock.name} : {exc}')

    if not locks:
        log.info('Aucun lock orphelin trouvé.')


def _warm_node_cache() -> None:
    """
    Préchauffage du cache XML de chaque opérateur SNAP, en série.
    Doit être appelé avant tout parallélisme pour éviter la contention sur les .lock.
    """
    log.info('Préchauffage du cache des nœuds SNAP...')
    for operator in _OPERATORS:
        log.debug(f'  cache : {operator}')
        parse_node(operator)
    log.info('Cache SNAP prêt.')


# ──────────────────────────────────────────────────────────────
# VALIDATION
# ──────────────────────────────────────────────────────────────

def validate_input_file(path: Path) -> None:
    if not path.exists():
        raise ValueError(f'Fichier introuvable : {path}')
    if not path.is_file():
        raise ValueError(f'Chemin invalide (pas un fichier) : {path}')
    if path.suffix.lower() != '.zip':
        raise ValueError(f'Le produit Sentinel-1 doit être un .zip : {path}')


def selected_polarisations(mode: str) -> list[str]:
    mode = mode.upper().strip()
    if mode == 'VV':
        return ['VV']
    if mode == 'VH':
        return ['VH']
    if mode == 'BOTH':
        return ['VV', 'VH']
    raise ValueError("POLARISATION_MODE doit être 'VV', 'VH' ou 'BOTH'")


def resolve_output_file(
    output_dir: Path,
    p1: Path,
    p2: Path,
    subswath: str,
    pol: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    name = f'{p1.stem}_{p2.stem}_{subswath.lower()}_{pol.lower()}_coreg.tif'
    return output_dir / name


# ──────────────────────────────────────────────────────────────
# NŒUDS SNAP (fonctions de construction)
# ──────────────────────────────────────────────────────────────

def add_read(workflow, product_path: Path):
    node = parse_node('Read')
    node.parameters['file'] = str(product_path)
    node.parameters['formatName'] = 'SENTINEL-1'
    workflow.insert_node(node)
    return node


def add_topsar_split(workflow, source_node, subswath, pol, first_burst=None, last_burst=None):
    node = parse_node('TOPSAR-Split')
    node.parameters['subswath'] = subswath
    node.parameters['selectedPolarisations'] = [pol]
    if first_burst is not None:
        node.parameters['firstBurstIndex'] = int(first_burst)
    if last_burst is not None:
        node.parameters['lastBurstIndex'] = int(last_burst)
    workflow.insert_node(node, before=source_node.id)
    return node


def add_apply_orbit_file(workflow, source_node):
    node = parse_node('Apply-Orbit-File')
    node.parameters['orbitType'] = 'Sentinel Precise (Auto Download)'
    node.parameters['polyDegree'] = 3
    node.parameters['continueOnFail'] = True
    workflow.insert_node(node, before=source_node.id)
    return node


def add_back_geocoding(workflow, src1, src2, mask_out_no_elevation: bool):
    node = parse_node('Back-Geocoding')
    node.parameters['demName'] = 'SRTM 1Sec HGT'
    node.parameters['maskOutAreaWithoutElevation'] = bool(mask_out_no_elevation)
    node.parameters['outputDerampDemodPhase'] = False
    workflow.insert_node(node, before=[src1.id, src2.id])
    return node


def add_enhanced_spectral_diversity(workflow, source_node):
    node = parse_node('Enhanced-Spectral-Diversity')
    node.parameters['fineWinWidthStr'] = '512'
    node.parameters['fineWinHeightStr'] = '512'
    node.parameters['xCorrThreshold'] = 0.1
    workflow.insert_node(node, before=source_node.id)
    return node


def add_topsar_deburst(workflow, source_node):
    node = parse_node('TOPSAR-Deburst')
    workflow.insert_node(node, before=source_node.id)
    return node


def add_multilook(workflow, source_node, range_looks=15, azimuth_looks=4):
    node = parse_node('Multilook')
    node.parameters['nRgLooks'] = range_looks
    node.parameters['nAzLooks'] = azimuth_looks
    node.parameters['outputIntensity'] = True
    workflow.insert_node(node, before=source_node.id)
    return node


def add_terrain_correction(workflow, source_node, crs: str):
    node = parse_node('Terrain-Correction')
    node.parameters['demName'] = 'SRTM 1Sec HGT'
    node.parameters['mapProjection'] = crs
    node.parameters['pixelSpacingInMeter'] = 20.0
    node.parameters['pixelSpacingInDegree'] = 0.00018
    node.parameters['saveSelectedSourceBand'] = True
    workflow.insert_node(node, before=source_node.id)
    return node


def add_write(workflow, source_node, output_tif: Path):
    node = parse_node('Write')
    node.parameters['file'] = str(output_tif)
    node.parameters['formatName'] = 'GeoTIFF'
    workflow.insert_node(node, before=source_node.id)
    return node


# ──────────────────────────────────────────────────────────────
# CONSTRUCTION DU GRAPHE COMPLET
# ──────────────────────────────────────────────────────────────

def build_workflow(
    p1: Path,
    p2: Path,
    tif_path: Path,
    subswath: str,
    pol: str,
    crs: str,
    first_burst=None,
    last_burst=None,
    mask_no_elev: bool = True,
):
    wf  = parse_recipe('blank')
    r1  = add_read(wf, p1)
    r2  = add_read(wf, p2)
    s1  = add_topsar_split(wf, r1, subswath, pol, first_burst, last_burst)
    s2  = add_topsar_split(wf, r2, subswath, pol, first_burst, last_burst)
    o1  = add_apply_orbit_file(wf, s1)
    o2  = add_apply_orbit_file(wf, s2)
    bg  = add_back_geocoding(wf, o1, o2, mask_no_elev)
    esd = add_enhanced_spectral_diversity(wf, bg)
    deb = add_topsar_deburst(wf, esd)
    ml  = add_multilook(wf, deb)
    tc  = add_terrain_correction(wf, ml, crs)
    add_write(wf, tc, tif_path)
    return wf


# ──────────────────────────────────────────────────────────────
# EXÉCUTION GPT avec retry
# ──────────────────────────────────────────────────────────────

def _run_gpt_once(recipe_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix='pyrosar_gpt_') as tmpdir:
        gpt(str(recipe_path), tmpdir=tmpdir, gpt_args=['-q', str(max(1, GPT_THREADS))])


def run_gpt_job(pair_id: str, pol: str, recipe_path: Path, tif_path: Path) -> JobResult:
    """Lance GPT pour un job donné avec jusqu'à GPT_MAX_RETRIES réessais."""
    last_error = ''
    for attempt in range(1 + GPT_MAX_RETRIES):
        if attempt > 0:
            log.warning(f'[{pair_id}/{pol}] Réessai {attempt}/{GPT_MAX_RETRIES}...')
        try:
            _run_gpt_once(recipe_path)
            log.info(f'[{pair_id}/{pol}] GPT terminé avec succès.')
            return JobResult(pair_id=pair_id, pol=pol, tif_path=tif_path, success=True)
        except Exception as exc:
            last_error = str(exc)
            log.error(f'[{pair_id}/{pol}] Échec tentative {attempt + 1} : {last_error}')

    return JobResult(
        pair_id=pair_id, pol=pol, tif_path=tif_path,
        success=False, error=last_error,
    )


# ──────────────────────────────────────────────────────────────
# CHARGEMENT XARRAY
# ──────────────────────────────────────────────────────────────

def load_as_xarray(result: JobResult) -> xr.Dataset:
    """
    Charge un GeoTIFF résultant en xarray.Dataset.

    Avec rioxarray (recommandé) :
      - coordonnées CRS complètes, méthodes .rio.reproject / .rio.clip disponibles

    Sans rioxarray (fallback rasterio) :
      - coordonnées x/y en unités de la projection, pas de méthodes géospatiales avancées
    """
    if not result.tif_path.exists():
        raise FileNotFoundError(f'TIF absent : {result.tif_path}')

    if _HAS_RIOXARRAY:
        da = xr.open_dataarray(result.tif_path, engine='rasterio')
    else:
        import numpy as np
        import rasterio
        with rasterio.open(result.tif_path) as src:
            data = src.read(1).astype('float32')
            t    = src.transform
            crs  = str(src.crs)
            y    = np.arange(src.height) * t.e + t.f
            x    = np.arange(src.width)  * t.a + t.c
        da = xr.DataArray(
            data,
            dims=['y', 'x'],
            coords={'y': y, 'x': x},
            name='backscatter',
            attrs={'crs': crs},
        )

    ds = da.to_dataset(name='backscatter')
    ds.attrs.update({
        'pair_id':      result.pair_id,
        'polarisation': result.pol,
        'source_tif':   str(result.tif_path),
    })
    return ds


# ──────────────────────────────────────────────────────────────
# PIPELINE
# ──────────────────────────────────────────────────────────────

def prepare_jobs(pairs: list[dict]) -> list[tuple]:
    """
    Valide les entrées et construit tous les graphes XML en série.
    Retourne une liste de tuples (pair_id, pol, recipe_path, tif_path).
    Le caractère séquentiel évite toute contention sur les locks pyrosar.
    """
    jobs: list[tuple] = []
    log.info(f'Préparation de {len(pairs)} paire(s)...')

    for i, pair in enumerate(pairs):
        p1          = pair['input_1']
        p2          = pair['input_2']
        subswath    = pair.get('subswath', 'IW1')
        pol_mode    = pair.get('polarisation_mode', 'VH')
        first_burst = pair.get('first_burst_index')
        last_burst  = pair.get('last_burst_index')
        pair_id     = f'pair{i + 1:03d}'

        validate_input_file(p1)
        validate_input_file(p2)

        for pol in selected_polarisations(pol_mode):
            tif_path    = resolve_output_file(OUTPUT_DIR, p1, p2, subswath, pol)
            recipe_path = tif_path.with_name(f'{tif_path.stem}_graph.xml')

            log.info(f'[{pair_id}/{pol}] Construction du graphe XML...')
            wf = build_workflow(
                p1, p2, tif_path,
                subswath=subswath, pol=pol,
                crs=OUTPUT_CRS,
                first_burst=first_burst,
                last_burst=last_burst,
                mask_no_elev=MASK_OUT_NO_ELEVATION,
            )
            wf.write(str(recipe_path))
            log.info(f'[{pair_id}/{pol}] Graphe écrit : {recipe_path.name}')
            jobs.append((pair_id, pol, recipe_path, tif_path))

    return jobs


def run_parallel(jobs: list[tuple]) -> list[JobResult]:
    """Lance les jobs GPT en parallèle (max GPT_WORKERS simultanés)."""
    results: list[JobResult] = []
    log.info(f'Lancement de {len(jobs)} job(s) GPT (workers={GPT_WORKERS})...')

    with tqdm(total=len(jobs), desc='Jobs GPT', unit='job') as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=GPT_WORKERS) as pool:
            futures = {
                pool.submit(run_gpt_job, pid, pol, rp, tp): (pid, pol)
                for pid, pol, rp, tp in jobs
            }
            for fut in concurrent.futures.as_completed(futures):
                pid, pol = futures[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    result = JobResult(
                        pair_id=pid, pol=pol, tif_path=Path(''),
                        success=False, error=str(exc),
                    )
                results.append(result)
                pbar.set_postfix_str(f'{pid}/{pol} {"OK" if result.success else "ERR"}')
                pbar.update(1)

    return results


def load_all_datasets(results: list[JobResult]) -> dict[str, xr.Dataset]:
    """
    Charge tous les TIF réussis en xarray.Dataset.
    Retourne un dict { 'pairXXX_POL': xr.Dataset }.
    """
    datasets: dict[str, xr.Dataset] = {}
    for r in results:
        if not r.success:
            log.warning(f'[{r.pair_id}/{r.pol}] Job échoué, ignoré. Erreur : {r.error}')
            continue
        try:
            ds = load_as_xarray(r)
            key = f'{r.pair_id}_{r.pol}'
            datasets[key] = ds
            log.info(f'[{r.pair_id}/{r.pol}] Dataset xarray chargé : {ds}')
        except Exception as exc:
            log.error(f'[{r.pair_id}/{r.pol}] Chargement xarray échoué : {exc}')
    return datasets


# ──────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ──────────────────────────────────────────────────────────────

def main() -> dict[str, xr.Dataset]:
    # Étape 1 — nettoyage préventif des locks orphelins
    _clean_stale_locks()

    # Étape 2 — préchauffage du cache SNAP (série, anti-contention)
    _warm_node_cache()

    # Étape 3 — construction de tous les graphes XML (série)
    jobs = prepare_jobs(PAIRS)

    # Étape 4 — exécution GPT en parallèle
    results = run_parallel(jobs)

    # Bilan
    n_ok  = sum(r.success for r in results)
    n_err = len(results) - n_ok
    log.info(f'Bilan : {n_ok} succès, {n_err} échec(s).')
    for r in results:
        if r.success:
            log.info(f'  ✓ {r.pair_id}/{r.pol} → {r.tif_path}')
        else:
            log.error(f'  ✗ {r.pair_id}/{r.pol} → {r.error}')

    # Étape 5 — chargement des résultats en xarray.Dataset
    return load_all_datasets(results)


if __name__ == '__main__':
    datasets = main()
    # `datasets` est un dict { 'pairXXX_POL': xr.Dataset }
    # Exemple d'utilisation downstream :
    #   ds = datasets['pair001_VH']
    #   backscatter = ds['backscatter']        # xr.DataArray (y, x)
    #   ds.to_netcdf('pile_temporelle.nc')     # export NetCDF
    #   ds.rio.reproject('EPSG:4326')          # reprojection (rioxarray requis)
