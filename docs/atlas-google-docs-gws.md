# Atlas Google Docs via `gws`

Hermes owns installation and unattended runtime wiring for Atlas's Google Docs adapter. It checksum-installs `gws` v0.22.5, stages only path references in `atlas-runtime.env`, and verifies the binary, authorized-user credential, private writable cache, and absence of Atlas legacy Google auth env. Atlas owns Drive invocation and document normalization.

## Security and ownership

- `/usr/local/bin/gws`: `root:root 0755`, exact v0.22.5, archive SHA-256 matched against the architecture-specific digest committed in `deploy.values.yaml`. A version bump requires review of upstream release notes/security advisories and a new committed digest; a sibling downloaded checksum is not trusted as the root of verification.
- `<HERMES_HOME>/auth/atlas-google-authorized-user.json`: staged out of band, `root:hermes 0640`, valid `authorized_user` shape. It is not created by the installer and is not passed in argv.
- `<HERMES_HOME>/cache/atlas-gws`: `hermes:hermes 0700`, writable for unattended refresh-token caching.
- Generic Hermes Google credentials and the Google Workspace skill are outside this migration. The installer neither rewrites nor removes `auth/google-sa-key.json`, its config, or any other non-Atlas credential.
- The old Atlas service-account file is not read or exported. It remains untouched during the credential-free implementation and canary, then must be revoked and deleted immediately after successful cutover.

`gws` is pre-1.0 and upstream marks it unsupported. Pinning plus fail-closed install/runtime verification prevents silent upgrades; security notices require an explicit reviewed pin bump.

## Release order and rollback

1. Merge and release Atlas with the no-fallback `gws` adapter. The installer refuses Atlas versions below `atlas.google_docs.minimum_atlas_version`.
2. Pin this repository's `atlas.version` to that release in the Hermes PR before merging/deploying it. This PR pins `v0.1.16` (Atlas merge `565c340feed86876f585ede241ff740b5d2910ef`).
3. Run the fake-fixture suites and obtain human approval to merge Hermes.
4. Create the Workspace-internal OAuth client/grant, stage the authorized-user credential, deploy Hermes, and run the installer verifier.
5. Run a real canary `files.get` + `files.export` through `atlas add` as `hermes`, monitor scheduled/unattended execution, then revoke/delete the retired Atlas service-account credential.

Do not deploy Hermes before Atlas `v0.1.16` is published and verified: its runtime env removes the only auth path understood by Atlas v0.1.15. Rollback is a coordinated release rollback—restore the prior Hermes release and prior Atlas binary together while the old production credential still exists. There is intentionally no runtime fallback or parallel auth path. After the old credential is retired, rollback requires an explicit new credential decision rather than silently resurrecting legacy auth.

## Required deployment inputs (not part of these PRs)

- Released Atlas tag/SHA containing the `gws` implementation, then the corresponding `atlas.version` bump here.
- Workspace-internal OAuth client ownership and consent policy for Otto's team-member account.
- Authorized-user credential created with `gws auth login --readonly --services drive` and staged at the configured path.
- Evidence, from the issued token/Admin console, that the grant contains Drive readonly and only the identity scopes that `gws` forces. The deployment owner must reject an external Testing-mode OAuth app because its refresh tokens expire after seven days; this is a human deployment gate, not something encoded in the authorized-user credential file.
- A representative team-owned Google Doc ID for the Krustentier canary, expected title/version, and an operator-approved canary window.
- Confirmation of the credential-revocation owner and monitoring/rotation contact.

OAuth creation/consent, credential staging, Krustentier canary, deployment, and old-key retirement are deliberately excluded from this implementation.
