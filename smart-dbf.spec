# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['src\\test\\test_controller.py'],
    pathex=[],
    binaries=[],
    datas=[('src/utils/data_tables_schemas.json', 'src/utils'), ('src/utils/mappings.json', 'src/utils'), ('src/utils/rules.json', 'src/utils'), ('src/utils/sql_identifiers.json', 'src/utils')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='smart-dbf',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
