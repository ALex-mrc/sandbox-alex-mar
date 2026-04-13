from pathlib import Path
import tempfile

from pyroSAR.snap.auxil import gpt, parse_recipe, parse_node
from tqdm import tqdm


INPUT_1 = Path(r'c:\Users\alexa\Downloads\S1A_IW_SLC__1SDV_20230818T154906_20230818T154932_049932_0601A0_FDD8.SAFE.zip')
INPUT_2 = Path(r'c:\Users\alexa\Downloads\S1A_IW_SLC__1SDV_20231029T154907_20231029T154934_050982_062582_3B98.SAFE.zip')
OUTPUT_DIR = Path(r'c:\Users\alexa\Downloads')
SUBSWATH = 'IW3'  # IW1, IW2 ou IW3
POLARISATION_MODE = 'VH'  # 'VV', 'VH' ou 'BOTH'
FIRST_BURST_INDEX = 3  # exemple: 3
LAST_BURST_INDEX = 7  # exemple: 7
OUTPUT_CRS = 'EPSG:32631'  # exemple: 'EPSG:32631'
MASK_OUT_NO_ELEVATION = True
WORKERS = 4


def selected_polarisations(mode: str) -> list[str]:
    mode = mode.upper().strip()
    if mode == 'VV':
        return ['VV']
    if mode == 'VH':
        return ['VH']
    if mode == 'BOTH':
        return ['VV', 'VH']
    raise ValueError("POLARISATION_MODE doit etre 'VV', 'VH' ou 'BOTH'")


def validate_input_file(path: Path) -> None:
    if not path.exists():
        raise ValueError(f'Fichier introuvable: {path}')
    if not path.is_file():
        raise ValueError(f'Chemin invalide (pas un fichier): {path}')
    if path.suffix.lower() != '.zip':
        raise ValueError(f'Le produit Sentinel-1 doit etre un .zip: {path}')


def resolve_output_file(output_dir: Path, product_1: Path, product_2: Path, subswath: str, pol: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    name = f'{product_1.stem}_{product_2.stem}_{subswath.lower()}_{pol.lower()}_coreg.tif'
    return output_dir / name


def add_read(workflow, product_path: Path):
    node = parse_node('Read')
    node.parameters['file'] = str(product_path)
    node.parameters['formatName'] = 'SENTINEL-1'
    workflow.insert_node(node)
    return node


def add_topsar_split(workflow, source_node, subswath: str, pol: str, first_burst_index=None, last_burst_index=None):
    node = parse_node('TOPSAR-Split')
    node.parameters['subswath'] = subswath
    node.parameters['selectedPolarisations'] = [pol]

    if first_burst_index is not None:
        node.parameters['firstBurstIndex'] = int(first_burst_index)
    if last_burst_index is not None:
        node.parameters['lastBurstIndex'] = int(last_burst_index)

    workflow.insert_node(node, before=source_node.id)
    return node


def add_apply_orbit_file(workflow, source_node):
    node = parse_node('Apply-Orbit-File')
    node.parameters['orbitType'] = 'Sentinel Precise (Auto Download)'
    node.parameters['polyDegree'] = 3
    node.parameters['continueOnFail'] = True
    workflow.insert_node(node, before=source_node.id)
    return node


def add_back_geocoding(workflow, source_node_1, source_node_2, mask_out_no_elevation: bool):
    node = parse_node('Back-Geocoding')
    node.parameters['demName'] = 'SRTM 1Sec HGT'
    node.parameters['maskOutAreaWithoutElevation'] = bool(mask_out_no_elevation)
    node.parameters['outputDerampDemodPhase'] = False
    workflow.insert_node(node, before=[source_node_1.id, source_node_2.id])
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


def build_workflow(
    product_1: Path,
    product_2: Path,
    output_tif: Path,
    subswath: str,
    pol: str,
    output_crs: str,
    first_burst_index=None,
    last_burst_index=None,
    mask_out_no_elevation=True,
    progress=None,
):
    workflow = parse_recipe('blank')

    def tick(step_name: str):
        if progress is not None:
            progress.set_description(f'Workflow {pol}: {step_name}')
            progress.update(1)

    read_1 = add_read(workflow, product_1)
    tick('Read 1')
    read_2 = add_read(workflow, product_2)
    tick('Read 2')

    split_1 = add_topsar_split(workflow, read_1, subswath, pol, first_burst_index, last_burst_index)
    tick('TOPSAR-Split 1')
    split_2 = add_topsar_split(workflow, read_2, subswath, pol, first_burst_index, last_burst_index)
    tick('TOPSAR-Split 2')

    orbit_1 = add_apply_orbit_file(workflow, split_1)
    tick('Apply-Orbit-File 1')
    orbit_2 = add_apply_orbit_file(workflow, split_2)
    tick('Apply-Orbit-File 2')

    back_geo = add_back_geocoding(workflow, orbit_1, orbit_2, mask_out_no_elevation)
    tick('Back-Geocoding')
    esd = add_enhanced_spectral_diversity(workflow, back_geo)
    tick('Enhanced-Spectral-Diversity')
    deburst = add_topsar_deburst(workflow, esd)
    tick('TOPSAR-Deburst')
    multilook = add_multilook(workflow, deburst)
    tick('Multilook')
    terrain = add_terrain_correction(workflow, multilook, output_crs)
    tick('Terrain-Correction')
    add_write(workflow, terrain, output_tif)
    tick('Write')

    return workflow


def run_gpt_workflow(recipe_path: Path, workers: int):
    with tempfile.TemporaryDirectory(prefix='pyrosar_gpt_') as tmpdir:
        gpt(str(recipe_path), tmpdir=tmpdir, gpt_args=['-q', str(max(1, workers))])


def main():
    validate_input_file(INPUT_1)
    validate_input_file(INPUT_2)

    polarisations = selected_polarisations(POLARISATION_MODE)

    for pol in polarisations:
        output_tif = resolve_output_file(OUTPUT_DIR, INPUT_1, INPUT_2, SUBSWATH, pol)
        recipe_path = output_tif.with_name(f'{output_tif.stem}_graph.xml')

        with tqdm(total=12, desc=f'Workflow {pol}', unit='step') as wf_progress:
            workflow = build_workflow(
                INPUT_1,
                INPUT_2,
                output_tif,
                subswath=SUBSWATH,
                pol=pol,
                output_crs=OUTPUT_CRS,
                first_burst_index=FIRST_BURST_INDEX,
                last_burst_index=LAST_BURST_INDEX,
                mask_out_no_elevation=MASK_OUT_NO_ELEVATION,
                progress=wf_progress,
            )

        workflow.write(str(recipe_path))

        with tqdm(total=1, desc=f'Execution GPT {pol}', unit='job') as run_progress:
            run_gpt_workflow(recipe_path, workers=WORKERS)
            run_progress.update(1)

        print(f'Sortie TIF ({pol}) : {output_tif}')
        print(f'Recette SNAP ({pol}) : {recipe_path}')

    print('Traitement termine avec succes.')


if __name__ == '__main__':
    main()
