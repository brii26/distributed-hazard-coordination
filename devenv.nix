{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

let
  # Kubernetes JSON schemas for offline manifest checks (kubeconform), fetched by Nix.
  k8sSchemaVer = "v1.34.1-standalone-strict";
  # Pinned commit of yannh/kubernetes-json-schema (a branch name like master is mutable).
  k8sSchemaRev = "6ea44f54bb5cdedb2f61fba42c9b342e97959262";
  k8sSchemaHashes = {
    "deployment-apps-v1" = "sha256-krejM6SRJPUwAJXT53hsogOLfW5gLDCfc+kGMTd+Swo=";
    "daemonset-apps-v1" = "sha256-fNQY+BVhxg0G+r6bbRFF8q7/jtGSwMncL7KAKlH3jCE=";
    "service-v1" = "sha256-i/AZhU2u1RHnwXSJaomBc/pl2I7Fk3xoejcwPUzJNRs=";
    "configmap-v1" = "sha256-4Ord69Z3wIqgkrLaImTYasT8NO7RErn6wpRbPwDB6bE=";
    "namespace-v1" = "sha256-Mk+uZ3uY0abVQ0DbDDNNBT6P+6/Os/czJuQd4mENWEM=";
    "secret-v1" = "sha256-3nYcywa9HSxkH7Vzl/YbaOf/U6OE8IF85A5d7YzuhvM=";
    "serviceaccount-v1" = "sha256-gZPWw1YUdcbT1cROH67bHfU5BTc9kEvBcBVpQybWWc8=";
    "clusterrole-rbac-v1" = "sha256-B3f5HE+gi9OYm224xZ2tBCmmPeyPWeiD/1XJR6FNDV8=";
    "clusterrolebinding-rbac-v1" = "sha256-4tBHG0RM9+pP1p1+uhsW4GIVUDv7xlBFDCtq2bQPhjw=";
  };
  k8sSchemas = pkgs.linkFarm "k8s-schemas" (
    lib.mapAttrsToList (name: hash: {
      name = "${name}.json";
      path = pkgs.fetchurl {
        url = "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${k8sSchemaRev}/${k8sSchemaVer}/${name}.json";
        inherit hash;
      };
    }) k8sSchemaHashes
  );
in
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
    pkgs.graphviz
    pkgs.kubectl
    pkgs.kubeconform
    pkgs.jq
    pkgs.protobuf
    pkgs.protoc-gen-go
    pkgs.protoc-gen-go-grpc
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
  scripts.tex-build.exec = "for f in doc/latex/figures/*.dot; do [ -e \"$f\" ] && dot -Tpdf \"$f\" -o \"\${f%.dot}.pdf\"; done; latexmk -xelatex -cd -outdir=build -interaction=nonstopmode -synctex=1 doc/latex/main.tex && cp doc/latex/build/main.pdf doc/latex/build/IF4031_M1_Ingress.pdf";
  scripts.tex-watch.exec = "for f in doc/latex/figures/*.dot; do [ -e \"$f\" ] && dot -Tpdf \"$f\" -o \"\${f%.dot}.pdf\"; done; latexmk -xelatex -pvc -view=none -cd -outdir=build -interaction=nonstopmode -synctex=1 doc/latex/main.tex";
  # Validate every k8s manifest in deploy/ offline against the pinned schemas.
  scripts.k8s-check.exec = "kubeconform -strict -summary -schema-location '${k8sSchemas}/{{.ResourceKind}}{{.KindSuffix}}.json' $(git ls-files 'deploy/*.yaml' 'deploy/**/*.yaml')";
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
