def log(msg, type=""):
    print(f"{type} {msg}")
    with open("log/logger.log", "a") as file_object:
        # Append 'hello' at the end of file
        file_object.write(f"{type} {msg}\n")


def info(msg):
    log(msg, "[INFO]")


def error(msg):
    log(msg, "[ERROR]")
