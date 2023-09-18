
"""
main entry point for the dataverse CLI tool
"""

from dataverse.utils.setting import SystemSetting


def main():
    """Main entry point for the cli."""
    print("🌌 Hello Welcome to Dataverse! 🌌")
    print("=" * 50)
    print("We are still under construction for CLI!")
    print("=" * 50)
    print("QUARK - By Ducky 🦆")

    # set the system setting to CLI mode
    SystemSetting().IS_CLI = True