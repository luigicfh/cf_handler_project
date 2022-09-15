class ApiError(Exception):
    def __init__(self, status_code, message="Something went wrong, status code: {}") -> None:
        self.message = message.format(status_code)
        super().__init__(self.message)
