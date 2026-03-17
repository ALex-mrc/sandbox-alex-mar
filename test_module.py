modules_manquants = []

# lire le fichier txt
with open("modules.txt", "r") as f:
    modules = [line.strip() for line in f if line.strip()]

for module in modules:
    try:
        __import__(module)
    except ImportError:
        modules_manquants.append(module)

with open("modules_manquants.txt", "w") as f:
    for module in modules_manquants:
        f.write(module + "\n")






#########################################################################



from pathlib import Path
import importlib.metadata

def check_modules(input_file="modules.txt", output_file="modules_manquants.txt"):
    lignes = Path(input_file).read_text().splitlines()
    lignes = [l.strip() for l in lignes if l.strip()]

    manquants = []

    for ligne in lignes:
        # Séparer le nom et la version demandée
        for op in ("==", ">=", "<=", "!=", ">", "<"):
            if op in ligne:
                nom, version_requise = ligne.split(op, 1)
                operateur = op
                break
        else:
            nom, version_requise, operateur = ligne, None, None

        nom = nom.strip()

        try:
            __import__(nom)
            version_installee = importlib.metadata.version(nom)

            if version_requise:
                from packaging.version import Version
                v_inst = Version(version_installee)
                v_req  = Version(version_requise)

                ok = {
                    "==": v_inst == v_req,
                    ">=": v_inst >= v_req,
                    "<=": v_inst <= v_req,
                    ">":  v_inst >  v_req,
                    "<":  v_inst <  v_req,
                    "!=": v_inst != v_req,
                }[operateur]

                if ok:
                    print(f"✅ {nom} {version_installee} (requis : {operateur}{version_requise})")
                else:
                    print(f"⚠️  {nom} {version_installee} ≠ requis {operateur}{version_requise}")
                    manquants.append(ligne)
            else:
                print(f"✅ {nom} {version_installee}")

        except ImportError:
            print(f"❌ {nom} — non installé")
            manquants.append(ligne)

        except importlib.metadata.PackageNotFoundError:
            print(f"⚠️  {nom} importable mais version introuvable")

    Path(output_file).write_text("\n".join(manquants))
    print(f"\n{len(manquants)}/{len(lignes)} modules manquants/incompatibles → {output_file}")

check_modules()
