# Can retrieve DocumentClient directly from api module
# The implementation details are not available in this package
from .api import DocumentClient  # noqa: F401
# Have exceptions available by default when importing this package
from . import exception  # noqa: F401
