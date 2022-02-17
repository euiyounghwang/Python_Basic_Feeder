class bcolors:
    HEADER    = '\033[95m'
    MARGENTA  = '\033[35m'
    BLUE      = '\033[34m'
    YELLOW    = '\033[33m'
    GREEN     = '\033[32m'
    RED       = '\033[31m'
    CYAN      = '\033[36m'
    OKBLUE    = '\033[94m'
    OKGREEN   = '\033[92m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'


if __name__ == '__main__':
    print('\n')
    print("==========================================================")
    print(bcolors().BOLD + bcolors().YELLOW + " start : %s " % ('lib_create') + bcolors().ENDC)
    print("==========================================================")