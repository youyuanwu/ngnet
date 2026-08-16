# Templates

Copies to fill in, not documents to read.

| Template | Copy to | For |
| --- | --- | --- |
| [`machine.md`](machine.md) | `data/<machine-id>/README.md` | A machine that has never been measured before. |
| [`run.md`](run.md) | `data/<machine-id>/NN-<slug>.md` | One benchmark run. |

Both carry `README.md` links that resolve only once the file is in a machine directory, which
is where they are meant to end up. Instructions for both are in
[`../README.md`](../README.md); the rules a run has to follow before it is worth recording are
in [`../../running.md`](../../running.md).

Write `not recorded` for anything that was not captured at the time. A guessed field is worse
than a missing one, because a later reader cannot tell which it is.
