# My Stash Plugins
Plugins for extending [Stash](https://github.com/stashapp/stash), the open-source media organizer.

# Installation
Add this repository as a plugin source in Stash:

Go to Settings → Plugins → Available Plugins

Click Add Source

Enter URL: ``` https://druidblack.github.io/stash-plugin/main/index.yml ```

Click Reload

# Open in Jellyfin 1.0.0
Adds a button to the interface that opens a link to jellyfin

<img width="771" height="147" alt="545563538-f40d4815-88b5-4c77-bfec-ef068032c049" src="https://github.com/user-attachments/assets/12789fce-6f85-41d6-8704-769acd76652c" />
<img width="452" height="225" alt="545563782-8e4f9240-b045-4498-b266-8040f81918f9" src="https://github.com/user-attachments/assets/6431bb37-8c83-4bfe-ad1b-be4568b6b2e6" />



[Open in Jellyfin](https://github.com/Druidblack/stash-plugin/tree/main/plugins/open_in_jellyfin)


# Jellyfin sync 0.2.13

A plugin that, when updating scene data in stash, sends a request for a point rescan of the video to update the metadata.

To add data, you need the [Jellyfin.Plugin.Stash](https://github.com/DirtyRacer1337/Jellyfin.Plugin.Stash) plugin.

The plugin can add a link to jellyfin to the stash data.

In order for spot scanning to work, the video must already have been added to jellyfin (it may not have metadata, the main thing is that it has a name).

<img width="691" height="902" alt="545604929-88299a26-47a5-4ed3-b846-5879f0412689" src="https://github.com/user-attachments/assets/931c2ed2-5d2f-4a88-85aa-6b1fe38db59f" />


[Jellyfin sync](https://github.com/Druidblack/stash-plugin/tree/main/plugins/jellyfin_sync)
