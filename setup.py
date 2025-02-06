import os
import subprocess

def install_system_dependencies():
    print("Updating system packages...")
    subprocess.run(["sudo", "apt-get", "update"], check=True)
    
    print("Installing essential system dependencies...")
    packages = [
        "curl",
        "libopencv-dev",  # OpenCV C++ development package
        "build-essential",
        "cmake",  # Required for compiling C++ projects
        "python3-pip",
        "python3-venv"
    ]
    subprocess.run(["sudo", "apt-get", "install", "-y"] + packages, check=True)

def install_python_libraries():
    print("Installing Python libraries...")
    python_packages = [
        "python-telegram-bot",
        "tqdm",
        "pandas",
        "opencv-python"
    ]
    subprocess.run(["pip3", "install"] + python_packages, check=True)

def setup_azcopy():
    print("Setting up AzCopy...")
    azcopy_url = "https://aka.ms/downloadazcopy-v10-linux"
    subprocess.run(["curl", "-L", azcopy_url, "-o", "azcopy.tar.gz"], check=True)
    subprocess.run(["tar", "-xf", "azcopy.tar.gz"], check=True)
    subprocess.run(["sudo", "mv", "azcopy_linux_amd64_*/azcopy", "/usr/local/bin/"], check=True)
    subprocess.run(["rm", "-rf", "azcopy_linux_amd64_*", "azcopy.tar.gz"], check=True)
    print("AzCopy setup complete.")

def main():
    install_system_dependencies()
    install_python_libraries()
    setup_azcopy()
    print("Setup completed successfully.")

if __name__ == "__main__":
    main()
