from colors import colors

main_str     = colors.fg.lightblue + "Main"    + colors.end
passed_str   = colors.fg.green     + "PASSED"  + colors.end
failed_str   = colors.fg.red       + "FAILED"  + colors.end
skipped_str  = colors.fg.yellow    + "SKIPPED" + colors.end
active_str   = colors.fg.green     + "ACTIVE"  + colors.end
success_str  = colors.fg.green     + "SUCCESS"  + colors.end


utils_str     = colors.fg.yellow   + "Utils" + colors.end
builder_str   = colors.fg.cyan     + "Builder"  + colors.end
tester_str    = colors.fg.cyan     + "Tester"  + colors.end
deployer_str  = colors.fg.cyan     + "Deployer"  + colors.end
