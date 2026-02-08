from typing import Literal
from vstools import core, vs, depth, split, join


def xyz2rgb(
    clip: vs.VideoNode,
    gamma: float = 2.2,
    colorspace: Literal["709", "2020"] = "2020",
    vulkan: bool = False,
) -> vs.VideoNode:
    """Convert from XYZ color space to RGB color space with gamma correction."""
    expr_func = core.llvmexpr.VkExpr if vulkan else core.llvmexpr.Expr
    X_plane, Y_plane, Z_plane = split(depth(clip, 32))

    common_expr = """
        # Linearization
        X_raw = $x ** 2.6;
        Y_raw = $y ** 2.6;
        Z_raw = $z ** 2.6;

        # Denormalization
        # SMPTE ST 428-1: The peak luminance as shown in the transfer function equation is 52.37 cd/m2 . The extra headroom is reserved to 
        # accommodate a range of white points including D55, D61 and D65, while still supporting the reference luminance (L) of 48 
        # cd/m2 as specified in SMPTE ST 431-1,Table 5.2 for Digital Cinema-Screen Luminance Level, Chromaticity and Uniformity.
        X_lin = X_raw * (52.37 / 48);
        Y_lin = Y_raw * (52.37 / 48);
        Z_lin = Z_raw * (52.37 / 48);

        # Chromatic Adaptation (D65)
        X_d65 = 1.02449673 * X_lin + 0.01516354 * Y_lin - 0.01968852 * Z_lin;
        Y_d65 = 0.02561219 * X_lin + 0.97258631 * Y_lin - 0.00471635 * Z_lin;
        Z_d65 = 0.00638423 * X_lin + -0.01226808 * Y_lin + 1.14794245 * Z_lin;
    """

    # XYZ to Linear RGB
    if colorspace == "709":
        common_expr += """
            R_lin = 3.24045484 * X_d65 + -1.53713885 * Y_d65 + -0.49853155 * Z_d65;
            G_lin = -0.96926639 * X_d65 + 1.87601093 * Y_d65 + 0.04155608 * Z_d65;
            B_lin = 0.05564342 * X_d65 + -0.204025859 * Y_d65 + 1.05722516 * Z_d65;
        """
    elif colorspace == "2020":
        common_expr += """
            R_lin =  1.71665118 * X_d65 + -0.35567078 * Y_d65 + -0.25336628 * Z_d65;
            G_lin = -0.66668435 * X_d65 +  1.61648123 * Y_d65 +  0.01576854 * Z_d65;
            B_lin =  0.01763985 * X_d65 + -0.04277061 * Y_d65 +  0.94210312 * Z_d65;
        """

    # Gamma Correction
    expr_r = common_expr + f"RESULT = R_lin ** (1.0 / {gamma});"
    expr_g = common_expr + f"RESULT = G_lin ** (1.0 / {gamma});"
    expr_b = common_expr + f"RESULT = B_lin ** (1.0 / {gamma});"

    mapped_r = expr_func(clips=[X_plane, Y_plane, Z_plane], expr=[expr_r], infix=1)
    mapped_g = expr_func(clips=[X_plane, Y_plane, Z_plane], expr=[expr_g], infix=1)
    mapped_b = expr_func(clips=[X_plane, Y_plane, Z_plane], expr=[expr_b], infix=1)

    return join(
        mapped_r, mapped_g, mapped_b, family=vs.ColorFamily.RGB
    ).std.SetFrameProps(
        _Matrix=0, _Transfer=14, _Primaries=9 if colorspace == "2020" else 1
    )


src_path = "/path/to/file.mxf"

clip_dcp = core.MXFJ2KSource.Source(src_path)
clip_rgb = xyz2rgb(clip_dcp)

clip_rgb.set_output(0)