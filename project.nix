{
  makeGoProject,
  fetchNixpkgs,
}:

let
  pkgs = fetchNixpkgs { lockFile = ./project.lock; };
  envtestAssets = pkgs.runCommandLocal "karpenter-envtest-assets" { } ''
    mkdir -p $out
    ln -s ${pkgs.kubernetes}/bin/kube-apiserver $out/kube-apiserver
    ln -s ${pkgs.etcd}/bin/etcd $out/etcd
    ln -s ${pkgs.kubectl}/bin/kubectl $out/kubectl
  '';
in
makeGoProject {
  workspaceRoot = ./.;
  goLock = ./gobuild-nix.lock;
  inherit pkgs;
  env = pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
    KUBEBUILDER_ASSETS = envtestAssets;
  };
}
