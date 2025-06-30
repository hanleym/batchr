{ inputs, cell }: let
  inherit (inputs) std self cells nixpkgs;
  inherit (nixpkgs) dockerTools;
  crane = (inputs.crane.mkLib nixpkgs).overrideToolchain cells.core.rust.toolchain;

  version = self.dirtyRev or self.rev;

  batchr = crane.buildPackage {
    inherit version;
    pname = "batchr";
    meta.mainProgram = "batchr";

    src = std.incl self [
      "${self}/Cargo.lock"
      "${self}/Cargo.toml"
      "${self}/src"
    ];

    strictDeps = true;
  };
in {
  inherit batchr;
  default = batchr;
}
