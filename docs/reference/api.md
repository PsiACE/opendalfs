# API reference

The public API contains the generic filesystem, the standard S3 adapter, and
the standard-protocol conflict helper.
Inherited filesystem operations follow the
{class}`fsspec.spec.AbstractFileSystem` and
{class}`fsspec.asyn.AsyncFileSystem` contracts.

## Filesystem

```{eval-rst}
.. autoclass:: opendalfs.OpendalFileSystem
   :members:
   :show-inheritance:

.. autoclass:: opendalfs.S3FileSystem
```

## Protocol registration

```{eval-rst}
.. autofunction:: opendalfs.register_opendal_standard_protocols
```
