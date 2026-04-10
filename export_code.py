import os

OUTPUT_FILE = "project_dump.txt"
EXCLUDE_DIRS = {".venv", "venv", "__pycache__", ".git"}

def should_skip(path):
    return any(excluded in path for excluded in EXCLUDE_DIRS)

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    for root, dirs, files in os.walk("."):
        if should_skip(root):
            continue

        for file in files:
            if file.endswith((".py", ".txt", ".md", ".json")):
                filepath = os.path.join(root, file)

                out.write(f"\n===== {filepath} =====\n\n")

                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        out.write(f.read())
                except:
                    out.write("[Could not read file]\n")

print(f"Exported to {OUTPUT_FILE}")