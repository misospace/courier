"""Exercise the license gate using the actual CLI and temporary reports."""
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("check-go-license-report.py")
COURIER = "github.com/misospace/courier,https://example.test/LICENSE,AGPL-3.0\n"


class LicenseGateTests(unittest.TestCase):
    def run_gate(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            capture_output=True, text=True, check=False,
        )

    def test_reports(self):
        cases = [
            ("compatible", COURIER + "example.test/pkg,https://example.test/LICENSE,MIT\n", True),
            ("missing project", "example.test/pkg,url,MIT\n", False),
            ("wrong project license", COURIER.replace("AGPL-3.0", "MIT"), False),
            ("unknown", COURIER + "example.test/pkg,url,Unknown\n", False),
            ("incompatible", COURIER + "example.test/pkg,url,GPL-2.0-only\n", False),
            ("malformed", COURIER + "broken,row\n", False),
            ("empty", "", False),
        ]
        with tempfile.TemporaryDirectory() as directory:
            report = pathlib.Path(directory) / "report.csv"
            for name, contents, accepted in cases:
                with self.subTest(name=name):
                    report.write_text(contents, encoding="utf-8")
                    result = self.run_gate(str(report))
                    self.assertEqual(result.returncode == 0, accepted, result.stderr)

    def test_missing_argument(self):
        result = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Usage:", result.stderr)

    def test_unreadable_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_gate(str(pathlib.Path(directory) / "missing.csv"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Cannot read Go license report", result.stderr)


if __name__ == "__main__":
    unittest.main()
