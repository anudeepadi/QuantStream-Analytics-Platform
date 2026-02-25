#!/usr/bin/env bash
# dev-session.sh - Launch a tmux dev session with 4 panes in iTerm2
# Panes: Docker Compose (API + infra) | Frontend (Next.js) | Claude Code | Local Bash

set -euo pipefail

SESSION_NAME="quantstream-dev"
PROJECT_DIR="/Users/vuc229/Documents/Development/Active-Projects/specialized/QuantStream-Analytics-Platform"

# Kill existing session if present
tmux has-session -t "$SESSION_NAME" 2>/dev/null && tmux kill-session -t "$SESSION_NAME"

launch_tmux_session() {
    # Create session — first pane becomes {top-left}
    tmux new-session -d -s "$SESSION_NAME" -c "$PROJECT_DIR"
    tmux rename-window -t "${SESSION_NAME}" "dev"

    # Pane 0 (top-left): Docker Compose (Postgres, Redis, Kafka, API, monitoring)
    tmux send-keys -t "${SESSION_NAME}:dev" \
        "docker compose up" C-m

    # Split top-left horizontally → creates pane 1 (top-right)
    tmux split-window -h -t "${SESSION_NAME}:dev" -c "${PROJECT_DIR}/src/dashboard/frontend-next"

    # Pane 1 (top-right): Frontend (Next.js)
    tmux send-keys -t "${SESSION_NAME}:dev.{right}" \
        "npm run dev" C-m

    # Split top-left vertically → creates pane below it (Claude Code)
    tmux split-window -v -t "${SESSION_NAME}:dev.{top-left}" -c "$PROJECT_DIR"

    # Pane 2 (bottom-left): Claude Code
    tmux send-keys -t "${SESSION_NAME}:dev.{bottom-left}" \
        "claude" C-m

    # Split top-right vertically → creates pane below it (Local Bash)
    tmux split-window -v -t "${SESSION_NAME}:dev.{top-right}" -c "$PROJECT_DIR"

    # Pane 3 (bottom-right): Local Bash
    tmux send-keys -t "${SESSION_NAME}:dev.{bottom-right}" \
        "" ""

    # Even out the 2x2 grid
    tmux select-layout -t "${SESSION_NAME}:dev" tiled

    # Focus on the local bash pane
    tmux select-pane -t "${SESSION_NAME}:dev.{bottom-right}"
}

open_in_iterm() {
    local tmux_cmd="tmux attach-session -t $SESSION_NAME"

    osascript <<EOF
tell application "iTerm2"
    if (count of windows) > 0 then
        tell current window
            create tab with default profile
            tell current session of current tab
                write text "$tmux_cmd"
            end tell
        end tell
    else
        create window with default profile
        tell current session of current window
            write text "$tmux_cmd"
        end tell
    end if
    activate
end tell
EOF
}

# Build the tmux session
launch_tmux_session

# Open in iTerm2 if it's installed, otherwise attach directly
if [ -d "/Applications/iTerm.app" ]; then
    open_in_iterm
    echo "Launched tmux session '$SESSION_NAME' in iTerm2"
else
    echo "iTerm2 not found, attaching in current terminal..."
    tmux attach-session -t "$SESSION_NAME"
fi
