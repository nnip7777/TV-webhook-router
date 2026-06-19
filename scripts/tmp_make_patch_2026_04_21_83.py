from pathlib import Path
version='2026.04.21-83'
Path('VERSION').write_text(version+'\n')
print(version)
