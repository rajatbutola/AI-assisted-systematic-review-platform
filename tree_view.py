import os

EXCLUDE_DIRS = {".venv", "venv", "__pycache__", ".git", ".idea"}
EXCLUDE_FILES = {".pyc", ".log"}

def should_exclude(name):
    return (
        name in EXCLUDE_DIRS or
        any(name.endswith(ext) for ext in EXCLUDE_FILES)
    )

def print_tree(start_path=".", indent="", max_depth=3, current_depth=0):
    if current_depth > max_depth:
        return

    try:
        items = sorted(os.listdir(start_path))
    except PermissionError:
        return

    for item in items:
        if should_exclude(item):
            continue

        path = os.path.join(start_path, item)
        print(indent + "|-- " + item)

        if os.path.isdir(path):
            print_tree(
                path,
                indent + "    ",
                max_depth,
                current_depth + 1
            )

print_tree(".")