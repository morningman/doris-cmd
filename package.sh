#!/bin/bash

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect operating system type
OS_TYPE=$(uname -s)

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Function to build standalone binary
build_standalone_binary() {
    echo -e "${BLUE}Detected OS: ${OS_TYPE}${NC}"

    # Display title
    echo -e "${GREEN}=======================================${NC}"
    echo -e "${GREEN}Apache Doris Command Line Interface Builder${NC}"
    echo -e "${GREEN}=======================================${NC}"
    echo ""

    # Check if Python is installed
    echo -e "${YELLOW}Checking for Python 3.6 or higher...${NC}"
    python_cmd=""
    for cmd in python3 python
    do
        if command -v $cmd &> /dev/null; then
            version=$($cmd -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
            major=$(echo $version | cut -d. -f1)
            minor=$(echo $version | cut -d. -f2)
            
            if [ "$major" -ge 3 ] && [ "$minor" -ge 6 ]; then
                python_cmd=$cmd
                echo -e "${GREEN}Found Python $version${NC}"
                break
            else
                echo -e "${YELLOW}Found Python $version, but version 3.6+ is required${NC}"
            fi
        fi
    done

    if [ -z "$python_cmd" ]; then
        echo -e "${RED}Error: Python 3.6 or higher is required but not found.${NC}"
        echo -e "${YELLOW}Please install Python 3.6+ before continuing.${NC}"
        return 1
    fi

    # Check if pip is installed
    echo -e "${YELLOW}Checking for pip...${NC}"
    pip_cmd=""
    for cmd in pip3 pip
    do
        if command -v $cmd &> /dev/null; then
            pip_cmd=$cmd
            echo -e "${GREEN}Found $cmd${NC}"
            break
        fi
    done

    if [ -z "$pip_cmd" ]; then
        echo -e "${RED}Error: pip is required but not found.${NC}"
        echo -e "${YELLOW}Please install pip before continuing.${NC}"
        return 1
    fi

    # Install PyInstaller and all dependencies
    echo -e "${YELLOW}Installing PyInstaller and dependencies...${NC}"
    $pip_cmd install --upgrade pyinstaller
    $pip_cmd install -r requirements.txt

    # Check if PyInstaller installed successfully
    if ! command -v pyinstaller &> /dev/null; then
        echo -e "${RED}Error: PyInstaller installation failed.${NC}"
        return 1
    fi

    # Create build and dist directories
    mkdir -p build dist

    # Determine output filename (based on OS)
    if [ "$OS_TYPE" = "Darwin" ]; then
        OUTPUT_NAME="doris-cmd-macos"
        echo -e "${BLUE}Building for macOS...${NC}"
    elif [ "$OS_TYPE" = "Linux" ]; then
        OUTPUT_NAME="doris-cmd-linux"
        echo -e "${BLUE}Building for Linux...${NC}"
    else
        OUTPUT_NAME="doris-cmd"
        echo -e "${YELLOW}Building for unknown OS...${NC}"
    fi

    # Get version number - using a simpler Python script approach
    VERSION=$($python_cmd -c 'import re; f=open("doris_cmd/__init__.py"); content=f.read(); f.close(); match=re.search(r"__version__\s*=\s*\"([^\"]+)\"", content); print(match.group(1) if match else "0.0.0")')

    # Set complete output filename with version
    OUTPUT_NAME="${OUTPUT_NAME}-${VERSION}"
    echo -e "${GREEN}Building version: ${VERSION}${NC}"

    # Run PyInstaller to build executable
    echo -e "${YELLOW}Building standalone executable...${NC}"
    pyinstaller --clean --onefile \
        --add-data "doris_cmd:doris_cmd" \
        --distpath "./dist" \
        --workpath "./build" \
        --name "$OUTPUT_NAME" \
        --hidden-import=rich \
        --hidden-import=tabulate \
        --hidden-import=prompt_toolkit \
        --hidden-import=pygments \
        --hidden-import=pymysql \
        doris_cmd/main.py

    # Check if build was successful
    if [ $? -ne 0 ]; then
        echo -e "${RED}Build failed!${NC}"
        return 1
    fi

    # Check if executable was generated
    if [ ! -f "./dist/$OUTPUT_NAME" ]; then
        echo -e "${RED}Build failed: Executable not found!${NC}"
        return 1
    fi

    # Add execution permissions
    chmod +x "./dist/$OUTPUT_NAME"

    # Display build results
    echo -e "${GREEN}=======================================${NC}"
    echo -e "${GREEN}Build successful!${NC}"
    echo -e "${GREEN}Executable: ./dist/${OUTPUT_NAME}${NC}"
    echo -e "${GREEN}=======================================${NC}"

    # Create symbolic link (optional)
    read -p "Create symbolic link 'doris-cmd' to the executable? (y/n) " -n 1 -r
    echo    # New line
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -L "./dist/doris-cmd" ]; then
            rm "./dist/doris-cmd"
        fi
        ln -s "./$OUTPUT_NAME" "./dist/doris-cmd"
        echo -e "${GREEN}Symbolic link created: ./dist/doris-cmd${NC}"
    fi
    
    return 0
}

# Function to create source distribution package
create_source_dist() {
    echo -e "${BLUE}Creating source distribution package...${NC}"
    
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Error: Python 3 is required but not found.${NC}"
        return 1
    fi
    
    # Create source distribution package
    echo -e "${YELLOW}Creating source distribution...${NC}"
    python3 setup.py sdist
    
    # Create wheel package
    echo -e "${YELLOW}Creating wheel package...${NC}"
    python3 setup.py bdist_wheel
    
    # Display generated files
    echo -e "${GREEN}Packages created:${NC}"
    ls -la "${SCRIPT_DIR}/dist/"
    
    return 0
}

# Display title
echo -e "${GREEN}=======================================${NC}"
echo -e "${GREEN}Apache Doris Command Line Interface Packager${NC}"
echo -e "${GREEN}=======================================${NC}"
echo ""

# Display options menu
echo -e "${YELLOW}Select the build option:${NC}"
echo -e "1) ${GREEN}Build standalone binary for current platform${NC} (using PyInstaller)"
echo -e "2) ${GREEN}Create source distribution package${NC} (pip installable)"
echo -e "3) ${GREEN}Exit${NC}"
echo ""

# Read user selection
read -p "Enter your choice [1-3]: " choice

case $choice in
    1) 
        build_standalone_binary
        ;;
    2)
        create_source_dist
        ;;
    3)
        echo -e "${GREEN}Exiting.${NC}"
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid option. Exiting.${NC}"
        exit 1
        ;;
esac

echo -e "\n${GREEN}=======================================${NC}"
echo -e "${GREEN}Package creation completed.${NC}"
echo -e "${GREEN}See the dist/ directory for generated files.${NC}"
echo -e "${GREEN}=======================================${NC}"

exit 0 