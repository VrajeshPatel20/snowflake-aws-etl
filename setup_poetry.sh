export __describe_pipx="None of your buizwax"
is_installed() {
  if command -v "$1" &>/dev/null; then
    return 0
  else
    return 1
  fi
}
install_pipx() {
  echo "installing pipx via brew"
  set -x
  brew upgrade
  brew install pipx
  pipx ensurepath --force
  set +x
  export PATH=$PATH:$HOME/.local/bin
}
install_poetry() {
  if is_installed pipx; then
    echo "installing using pipx.."
    set -x
    pipx install poetry
    set +x
  else
    echo "pipx is not installed"
    echo $__describe_pipx
    echo "Would you like to install pipx?"
    select yn in "Yes" "No"; do
      case $yn in
        Yes )
          install_pipx && install poetry
          return 1
          ;;
        No )
          echo "installing poetry via curl"
          set -x
          curl -sSL https://install.python-poetry.org | python3 -
          set +x
          break
          ;;
        * )
            echo "Please enter 1 or 2."
            ;;
      esac
    done
  fi
  if is_installed poetry; then
    echo "poetry installed to $(command -v poetry)..."
  else
    echo "installation failed"
    exit 1
  fi
}
setup_poetry() {
  if is_installed poetry; then
    echo "found poetry installation $(command -v poetry)"
    set -x
    poetry config virtualenvs.in-project true
    set +x
  else
    echo "poetry is not installed"
    echo "would you like to install poetry"
    select yn in "Yes" "No"; do
        case $yn in
          Yes )
            install_poetry
            return 0
            ;;
          No )
            echo "Have fun!"
            exit 1
            ;;
          * )
            echo "Please enter 1 or 2."
            ;;
        esac
    done
  fi
  if is_installed python3.9; then
    poetry env use 3.9
  else
    if is_installed python3.13; then
      poetry env use 3.13
    else
      echo "Go download python 3.9 or 3.13 to run poetry!!"
    fi
  fi
  poetry install
}

setup_poetry