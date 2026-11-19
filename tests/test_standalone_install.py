import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1]


class StandaloneInstallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        code = (SOURCE / "install.sh").read_text().split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        cls.installer = {"__name__": "standalone_installer_test"}
        exec(compile(code, "standalone_install.py", "exec"), cls.installer)

    def test_unit_keeps_paths_raw_and_quotes_exec_arguments(self):
        prefix = Path("/opt/relay folder")
        template = (SOURCE / "boluo-qq-relay.service.in").read_text()
        unit = self.installer["render_unit"](template, prefix)
        fields = dict(line.split("=", 1) for line in unit.splitlines() if "=" in line)
        self.assertEqual(fields["WorkingDirectory"], str(prefix))
        self.assertEqual(fields["EnvironmentFile"], "-" + str(prefix / "token.env"))
        self.assertEqual(shlex.split(fields["ExecStart"]), [
            str(prefix / ".venv/bin/python"), str(prefix / "relay.py"),
            "--config", str(prefix / "config.json"),
        ])
        self.assertNotIn("@", unit)

    def test_unexpanded_unit_placeholder_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.installer["render_unit"]("ExecStart=@UNKNOWN@", Path("/opt/relay"))

    def test_paths_that_can_expand_or_inject_unit_fields_are_rejected(self):
        for suffix in (' ', '\nRestart=no', '*', '?', '[abc]', '%h', '$TASK', '"', '\\'):
            with self.subTest(suffix=suffix), self.assertRaises(SystemExit):
                self.installer["safe_path"]("/opt/relay" + suffix)

    def test_shell_installer_dry_run_does_not_create_install_or_unit(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            prefix = directory / "relay folder"
            settings = directory / "xdg"
            environment = dict(os.environ, XDG_CONFIG_HOME=str(settings))
            result = subprocess.run([
                "sh", str(SOURCE / "install.sh"), "--dry-run", "--prefix", str(prefix),
            ], env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("安装计划检查通过", result.stdout)
            self.assertFalse(prefix.exists())
            self.assertFalse(settings.exists())


if __name__ == "__main__":
    unittest.main()
