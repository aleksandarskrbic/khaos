Output demo.gif

Set Shell "zsh"
Set FontSize 16
Set Width 1200
Set Height 700
Set Theme "Dracula"
Set Padding 20
Set Framerate 60
Set TypingSpeed 50ms

# List available scenarios
Type "khaos list"
Enter
Sleep 4s

# Run scenario
Type "khaos run traffic/high-throughput -d 5 -k"
Enter
Sleep 12s

# Show cluster status
Type "khaos cluster-status"
Enter
Sleep 5s

# Cleanup
Type "khaos cluster-down"
Enter
Sleep 5s
