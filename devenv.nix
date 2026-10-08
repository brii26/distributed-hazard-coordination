{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.gh
    pkgs.golangci-lint
    pkgs.pandoc
    pkgs.libreoffice
    pkgs.poppler-utils
  ];

  # https://devenv.sh/languages/
  languages.go.enable = true;
  languages.texlive.enable = true;
  languages.texlive.base = pkgs.texliveFull;
  dotenv.enable = true;

  # https://devenv.sh/processes/
  # processes.dev.exec = "${lib.getExe pkgs.watchexec} -n -- ls -la";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/scripts/
  scripts.tex-build.exec = "latexmk -xelatex -cd -outdir=build -interaction=nonstopmode -synctex=1 doc/latex/main.tex";
  scripts.tex-watch.exec = "latexmk -xelatex -pvc -view=none -cd -outdir=build -interaction=nonstopmode -synctex=1 doc/latex/main.tex";
  # scripts.hello.exec = ''
  #   echo hello from $GREET
  # '';

  # https://devenv.sh/basics/
  # enterShell = ''
  #   hello         # Run scripts directly
  #   git --version # Use packages
  # '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  # enterTest = ''
  #   echo "Running tests"
  #   git --version | grep --color=auto "${pkgs.git.version}"
  # '';

  # https://devenv.sh/git-hooks/
  # git-hooks.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
