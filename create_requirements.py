import subprocess
import re

def generate_requirements(project_path="."):
    """
    Генерация файла requirements.txt для текущего окружения,
    включая только библиотеки, используемые в проекте, и очистка от дубликатов.

    Args:
        project_path (str): Путь к проекту (по умолчанию текущая директория).
    """
    try:
        import pipreqs
    except ImportError:
        subprocess.check_call(["pip", "install", "pipreqs"])

    subprocess.run(["pipreqs", project_path, "--force"], check=True)

    # Удаляем 'pipreqs' из requirements и чистим дубликаты
    clean_requirements("requirements.txt", "requirements.txt", exclude=["pipreqs"])

    # Добавляем psycopg2==2.9.10 в requirements.txt
    add_library_to_requirements("psycopg2", "2.9.10", "requirements.txt")


def clean_requirements(input_file="requirements.txt", output_file="requirements.txt", exclude=None):
    """
    Удаляет дубликаты библиотек в requirements.txt, оставляя только самые последние версии,
    а также исключает указанные библиотеки.

    Args:
        input_file (str): Имя входного файла.
        output_file (str): Имя выходного файла.
        exclude (list): Список библиотек, которые нужно исключить из файла.
    """
    if exclude is None:
        exclude = []

    latest_versions = {}

    with open(input_file, "r") as file:
        for line in file:
            if not line.strip():
                continue
            
            match = re.match(r"([a-zA-Z0-9_\-]+)==([\d\.]+)", line)
            if match:
                library, version = match.groups()
                if library in exclude:
                    continue
                # Если библиотеки ещё нет или версия новее — обновляем
                if library not in latest_versions or version > latest_versions[library]:
                    latest_versions[library] = version

    with open(output_file, "w") as file:
        for library, version in sorted(latest_versions.items()):
            file.write(f"{library}=={version}\n")


def add_library_to_requirements(library, version, file="requirements.txt"):
    """
    Добавляет указанную библиотеку с версией в конец requirements.txt,
    если её там ещё нет, и убирает лишние пустые строки.
    """
    with open(file, "r") as f:
        lines = [line.rstrip() for line in f if line.strip()] 

    for line in lines:
        if line.startswith(f"{library}=="):
            return

    lines.append(f"{library}=={version}")

    with open(file, "w") as f:
        f.write("\n".join(lines) + "\n")  # Записываем без пустых строк


project_directory = "."
generate_requirements(project_directory)