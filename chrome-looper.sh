#! /bin/sh
while true
do
    google-chrome --incognito --new-window https://ingatlan.com/ && sleep 5 && xdotool key Ctrl+Shift+L && sleep $1 && xdotool key Ctrl+W && sleep 1 && xdotool key Ctrl+W && sleep 1 && xdotool key Ctrl+W && sleep $2
done
