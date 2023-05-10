cat > ~/.config/git/hooks/pre-commit << EOF
#!/bin/sh

# Check if there are staged changes
if [ -z "\$(git diff --cached --name-only)" ]; then
  echo "No staged changes found. Exiting pre-commit hook."
  exit 0
fi

# Aliyun Access Key pattern
ACCESS_KEY_PATTERN='LTA[0-9A-Za-z]{16}'

# ANSI color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No color

# Check if the commit contains Aliyun Access Key
if git diff --cached --name-only | xargs grep -E -n -e "\$ACCESS_KEY_PATTERN"; then
    printf "\${RED}Error: Commit contains possible Aliyun Access Key.\${NC}\n"
    printf "\${YELLOW}Please remove the sensitive information before committing.\${NC}\n"
    printf "\${YELLOW}The following lines contain sensitive information:\${NC}\n"
    git diff --cached --name-only | xargs grep --color=always -E -n -e "\$ACCESS_KEY_PATTERN"
    printf "\${RED}Commit aborted. Please review and modify the changes before committing again.\${NC}\n"
    printf "\${YELLOW}If you are sure the warning is a false positive, you can force commit by using the following command:\${NC}\n"
    printf "\${YELLOW}git commit --no-verify\${NC}\n"
    exit 1
fi
EOF
