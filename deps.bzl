"""
@generated by: bazel run //:gazelle-update-repos
"""

load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_dependencies():
    """
    Bazel dependencies from go.mod/go.sum.  Please user #keep comment to prevent changes from being overwritten.
    """

    go_repository(
        name = "com_github_cespare_xxhash_v2",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:DC2CZ1Ep5Y4k3ZQ899DldepgrayRUGE6BBZ/cd9Cj44=",
        version = "v2.2.0",
    )

    go_repository(
        name = "com_github_davecgh_go_spew",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_google_btree",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/google/btree",
        sum = "h1:xf4v41cLI2Z6FxbKm+8Bu+m8ifhj15JuZ9sa0jZCMUU=",
        version = "v1.1.2",
    )

    go_repository(
        name = "com_github_klauspost_compress",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/klauspost/compress",
        sum = "h1:2mk3MPGNzKyxErAw8YaohYh69+pa4sIQSC0fPGCFR9I=",
        version = "v1.16.7",
    )
    go_repository(
        name = "com_github_kr_text",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/kr/text",
        sum = "h1:5Nx0Ya0ZqY2ygV366QzturHI13Jq95ApcVaJBhpS+AY=",
        version = "v0.2.0",
    )

    go_repository(
        name = "com_github_pmezard_go_difflib",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_savetherbtz_fastcdc_go",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/SaveTheRbtz/fastcdc-go",
        sum = "h1:JdHvLlnijDuisYIwpRDcHZEjbxvCqtEmJ3gf35VJBgA=",
        version = "v0.3.0",
    )

    go_repository(
        name = "com_github_stretchr_objx",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/stretchr/objx",
        sum = "h1:1zr/of2m5FGMsad5YfcqgdqdWrIhu+EBEJRhR1U7z/c=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "github.com/stretchr/testify",
        sum = "h1:CcVxjf3Q8PM0mHUKJCdn+eZZtm5yQwehR5yeSVQQcUk=",
        version = "v1.8.4",
    )

    go_repository(
        name = "in_gopkg_check_v1",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "gopkg.in/check.v1",
        sum = "h1:yhCVgyC4o1eVCa2tZl7eS0r+SDo693bJlVdllGtEeKM=",
        version = "v0.0.0-20161208181325-20d25e280405",
    )

    go_repository(
        name = "in_gopkg_yaml_v3",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
    )

    go_repository(
        name = "org_uber_go_atomic",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "go.uber.org/atomic",
        sum = "h1:ZvwS0R+56ePWxUNi+Atn9dWONBPp/AUETXlHW0DxSjE=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_goleak",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "go.uber.org/goleak",
        sum = "h1:xqgm/S+aQvhWFTtR0XK3Jvg7z8kGV8P4X14IzwN3Eqk=",
        version = "v1.2.0",
    )
    go_repository(
        name = "org_uber_go_multierr",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "go.uber.org/multierr",
        sum = "h1:blXXJkSxSSfBVBlC76pxqeO+LN3aDfLQo+309xJstO0=",
        version = "v1.11.0",
    )
    go_repository(
        name = "org_uber_go_zap",
        build_directives = ["gazelle:exclude **/**_test.go", "gazelle:exclude testing", "gazelle:exclude **/testdata"],
        importpath = "go.uber.org/zap",
        sum = "h1:sI7k6L95XOKS281NhVKOFCUNIvv9e0w4BF8N3u+tCRo=",
        version = "v1.26.0",
    )
