#!/bin/bash

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Apache Doris Command Line Interface Build & Install Script ===${NC}"
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
    exit 1
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
    exit 1
fi

# Check for required packages
echo -e "${YELLOW}Checking for required packages...${NC}"

# First, try to install in user mode
echo -e "${GREEN}Building and installing doris-cmd and dependencies...${NC}"
$pip_cmd install --user -e .

# Check if installation was successful
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Local user installation failed, trying with sudo...${NC}"
    
    # Ask for sudo permission
    echo -e "${YELLOW}This operation requires administrator privileges.${NC}"
    
    # Try with sudo
    sudo $pip_cmd install -e .
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to build and install doris-cmd.${NC}"
        exit 1
    fi
fi

# Success message
echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}doris-cmd has been built and installed successfully!${NC}"
echo -e "${GREEN}Run 'doris-cmd --help' to get started.${NC}"
echo -e "${GREEN}=====================================${NC}"

exit 0 