class ExperimentError(Exception):
    """Base error for the bake-off harness."""


class MissingDependencyError(ExperimentError):
    def __init__(self, package: str, backend: str) -> None:
        self.package = package
        self.backend = backend
        super().__init__(f"backend {backend!r} needs {package}. Install it in this environment; do not add it to aspara's pyproject until a winner is chosen.")


class UnsupportedReplicaError(ExperimentError):
    def __init__(self, backend: str, mode: str, supported: tuple[str, ...]) -> None:
        self.backend = backend
        self.mode = mode
        self.supported = supported
        super().__init__(f"backend {backend!r} does not support replica mode {mode!r}. supported: {', '.join(supported)}. skip as 非対応.")


class BackendConfigError(ExperimentError):
    """Missing URL, token, path, or other connection settings."""
