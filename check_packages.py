import commands

def is_installed(package_name):
    return "not installed" in commands.getstatusoutput("rpm -q " + package_name)[1]

