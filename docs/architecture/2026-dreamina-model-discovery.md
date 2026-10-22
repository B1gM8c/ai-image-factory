# Dreamina CLI Model Discovery

The model catalog refresh inspects all eight generation workflows with `--help`.
Discovery runs only through the managed account's queued catalog refresh; it
does not run on generation requests. Each refresh permits two concurrent help
processes, within the existing limit of four concurrent refreshes. Existing
process timeouts and account home isolation apply.

CLI 1.4.21 help inspected on 2026-10-22 declares these model sets:

| Workflow | Declared models |
| --- | --- |
| `text2image` | `3.0`, `3.1`, `4.0`, `4.1`, `4.5`, `4.6`, `4.7`, `5.0`, `5.0Pro` |
| `image2image` | `4.0`, `4.1`, `4.5`, `4.6`, `4.7`, `5.0`, `5.0Pro` |
| `image_upscale` | Fixed model; Factory catalog key `image_upscale` |
| `text2video` | Seedance 2.x set below |
| `image2video` | `seedance1.0fast`, `seedance1.5pro`, plus the Seedance 2.x set |
| `frames2video` | `seedance1.5pro`, plus the Seedance 2.x set |
| `multiframe2video` | Fixed model; Factory catalog key `multiframe2video` |
| `multimodal2video` | Seedance 2.x set below |

The shared Seedance 2.x set is `seedance2.0`, `seedance2.0fast`,
`seedance2.0_vip`, `seedance2.0fast_vip`, `seedance2.0mini`, and `seedance2.5`.
The union contains nine selectable image models and eight selectable video models.
The two fixed workflows bring the Factory catalog to 19 entries; their catalog
keys select a workflow and are not passed as a CLI `model_version`.

Discovery understands the help's `- model_version:`,
`- model_version values:`, and `--model_version string supported values:`
declarations. It merges by media kind and model ID, retaining each model's
declared workflow names in `metadata_json.commands` on both the catalog and
account observation. Fixed workflows use their command names as catalog keys.
The CLI's JSON version output is recorded using its `version` field.

Every selectable-model workflow must return a readable, nonempty model list.
If a command fails or its model list cannot be parsed, the refresh fails and
leaves the previous observations intact instead of publishing a partial union.

All eight workflows have execution adapters, including the image-based
`seedance1.0fast` and `seedance1.5pro` models. Discovery does not automatically
enable an existing disabled entry, publish a price, or revise a project's route.
Those normal configuration steps determine availability in `/v1/models`.
A command listed in discovery metadata proves that CLI help declares it, not
that an account has completed a successful generation. See the
[workflow API guide](../operations/dreamina-cli-workflows.md) for parameters and
the boundary between local adapter tests and production generation evidence.

The focused unit tests cover the three declaration formats, the complete
19-entry catalog and per-model command membership, rejection of a partial
catalog, and executable legacy/fixed workflow mappings:

```sh
cargo test -p gpt-image-2-gateway --lib provider_management::model_catalog::tests --locked
```
