#!/bin/bash
set -e

echo "--- ANTIGRAVITY INIT STARTED ---"

# 1. FIX SOCKET (Prevent startup hang)
#    We use sudo because we are likely running as the 'vscode' user
if [ -S /var/run/docker-host.sock ]; then
    echo "Linking Docker socket..."
    sudo ln -sf /var/run/docker-host.sock /var/run/docker.sock
    sudo chmod 666 /var/run/docker.sock
fi


# 1b. FIX BROWSER TOOL (Forward CDP port to Chrome on host)
#     Antigravity connects to 127.0.0.1:9222 for Chrome DevTools Protocol.
#     We relay this to the host machine where Chrome actually runs.
echo "Starting CDP port forwarder (9222 → host)..."
(
    socat TCP-LISTEN:9222,fork,reuseaddr TCP:host.docker.internal:9222 2>/dev/null &
) &

# 2. FIX PATH (Prevent '127 Command Not Found')
#    Start the background watcher to fix the directory when Antigravity uploads it.
echo "Starting background path watcher..."
(
    while true; do
        # Find the "broken" folder (e.g. 1.16.5-1504c8...)
        TARGET=$(ls -d /home/vscode/.antigravity-server/bin/1.16.5-* 2>/dev/null | head -n 1)
        
        if [ -n "$TARGET" ]; then
            # Extract the clean commit hash
            LINK_NAME=$(basename "$TARGET" | cut -d- -f2-)
            
            # Create the symlink if it's missing
            if [ ! -d "/home/vscode/.antigravity-server/bin/$LINK_NAME" ]; then
                echo "Fixing path: $LINK_NAME -> $TARGET"
                ln -sf "$TARGET" "/home/vscode/.antigravity-server/bin/$LINK_NAME"
            fi
        fi
        sleep 2
    done
) &

echo "--- INIT COMPLETE. STARTING MAIN COMMAND ---"
# 3. HANDOFF (Crucial!)
#    This executes the command passed from Docker Compose (sleep infinity).
exec "$@"