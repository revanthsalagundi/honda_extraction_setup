import os
import subprocess


def install_packages():
    # List of required system packages
    packages = [
        "curl",
        "libopencv-dev",  # OpenCV C++ development package
        "build-essential",
        "cmake",  # Required for compiling C++ projects
        "python3-pip",
        "python3-venv",
    ]

    # Update and install packages
    print("Updating package list and installing required system packages...")
    os.system("sudo apt-get update -y")
    os.system(f"sudo apt-get install -y {' '.join(packages)}")

    print("System packages installed successfully.")


def install_python_dependencies():
    required_python_packages = ["pandas", "tqdm", "python-telegram-bot"]

    print("Installing required Python packages...")
    subprocess.check_call(
        [os.sys.executable, "-m", "pip", "install"] + required_python_packages
    )
    print("Python packages installed successfully.")


def setup_azcopy():
    print("Downloading and installing AzCopy...")
    try:
        os.system("wget https://aka.ms/downloadazcopy-v10-linux")
        os.system("tar -xvf downloadazcopy-v10-linux")
        os.system("sudo rm -f /usr/bin/azcopy")
        os.system("sudo cp ./azcopy_linux_amd64_*/azcopy /usr/bin/")
        os.system("sudo chmod 755 /usr/bin/azcopy")

        # Clean up
        os.system("rm -f downloadazcopy-v10-linux")
        os.system("rm -rf ./azcopy_linux_amd64_*/")

        print("AzCopy installed successfully.")
    except Exception as e:
        print(f"Error during AzCopy setup: {e}")


if __name__ == "__main__":
    install_packages()
    install_python_dependencies()
    setup_azcopy()
