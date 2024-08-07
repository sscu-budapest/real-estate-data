#! /bin/sh
git pull
python3 -c "from src.ingatlan_webext import collect;collect()" || exit 1
git add report.md 
git commit -m "webext run"
git push
