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
| `image_upscale` | Fixed model; no public model ID |
| `text2video` | Seedance 2.x set below |
| `image2video` | `seedance1.0fast`, `seedance1.5pro`, plus the Seedance 2.x set |
| `frames2video` | `seedance1.5pro`, plus the Seedance 2.x set |
| `multiframe2video` | Fixed model; no public model ID |
| `multimodal2video` | Seedance 2.x set below |

The shared Seedance 2.x set is `seedance2.0`, `seedance2.0fast`,
`seedance2.0_vip`, `seedance2.0fast_vip`, `seedance2.0mini`, and `seedance2.5`.
The union contains nine image models and eight video models.

Discovery understands the help's `- model_version:`,
`- model_version values:`, and `--model_version string supported values:`
declarations. It merges by media kind and model ID, retaining each model's
declared workflow names in `metadata_json.commands` on both the catalog and
account observation. The fixed-model workflows do not create synthetic model
IDs. The CLI's JSON version output is recorded using its `version` field.

Every selectable-model workflow must return a readable, nonempty model list.
If a command fails or its model list cannot be parsed, the refresh fails and
leaves the previous observations intact instead of publishing a partial union.

Discovery is distinct from adapter execution support. `seedance1.0fast` and
`seedance1.5pro` are catalog-only entries with `adapter_state=discovered` and
`lifecycle_state=disabled`; their list availability remains `not_supported`.
They receive no routable alias or new pricing rule from this change. A command
listed in discovery metadata proves that CLI help declares it, not that the
Factory adapter implements that workflow or that an account has completed a
successful generation. Full parameter and workflow adapter coverage remains
separate work.

The focused unit tests cover the three declaration formats, the complete
17-model union and per-model command membership, rejection of a partial
catalog, and the non-routable legacy-model boundary:

```sh
cargo test -p gpt-image-2-gateway --lib provider_management::model_catalog::tests --locked
```
