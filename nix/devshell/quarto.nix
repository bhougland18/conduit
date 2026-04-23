{ ... }:
{
  perSystem =
    {
      config,
      lib,
      pkgs,
      ...
    }:
    let
      cfg = config.dendritic.devShell.features.quarto;
    in
    {
      config = lib.mkIf cfg.enable {
        dendritic.devShell.packages = [ pkgs.quarto ];
      };
    };
}
