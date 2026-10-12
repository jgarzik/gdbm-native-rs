# Security policy

gdbm-native reads database files and dumps that may come from untrusted sources. Any way in
which such input can cause a panic, unbounded memory or CPU use, out-of-bounds file access or
corruption of other data is a security issue.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/jgarzik/gdbm-native-rs/security/advisories/new)
rather than in a public issue. Include a reproducing file or test if you can.

## Supported versions

Fixes are made to the latest release.
