API
===

Document Client
---------------

.. note::
    Although the :py:class:`DocumentClient` is implemented in the :py:mod:`api` module,
    :py:mod:`api` is not meant to be imported directly because it contains helper functions.
    Please import :py:class:`DocumentClient` from the :py:mod:`documentapi` package as shown in the cookbook.

.. autoclass:: documentapi.api.DocumentClient
    :members:

Exceptions
----------

.. automodule:: documentapi.exception
    :members:
