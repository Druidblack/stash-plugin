# My Stash Plugins
Plugins for extending [Stash](https://github.com/stashapp/stash), the open-source media organizer.

# Installation
Add this repository as a plugin source in Stash:

Go to Settings → Plugins → Available Plugins

Click Add Source

Enter URL: ``` https://druidblack.github.io/stash-plugin/main/index.yml ```

Click Reload


# Jellyfin sync 0.3.26

A plugin for synchronizing data from Stash to Jellyfin.
It can synchronize selected actors, their information, and the video playback position; it can generate a cover in Jellyfin format either in bulk or on a per‑actor basis.

For two-way synchronization, use [JF To Stash Sync](https://github.com/Druidblack/Jellyfin.Plugin.JF_To_Stash_Sync) plugin.

The plugin can add a link to jellyfin to the stash data.


[Jellyfin sync](https://github.com/Druidblack/stash-plugin/tree/main/plugins/jellyfin_sync)


Example

<img width="400" height="600" alt="scene_76740" src="https://github.com/user-attachments/assets/d2f518f5-72b9-42e4-8682-b63851ef22a0" />

The buttons to open the jellyfin link and generate an cover for jellyfin.

<img width="137" height="49" alt="buttom" src="https://github.com/user-attachments/assets/0c9b28e7-25da-4115-ba4b-c5de4153a535" />

Tasks

<img width="854" height="326" alt="задачи" src="https://github.com/user-attachments/assets/0b165d6f-ef64-4c3e-840a-8446cfe38820" />



# File Name Title Cheker 0.4.4

Checks scene titles against video filenames, compares configured GraphQL/Stash-box metadata sources, applies chosen metadata to Stash scenes, and can rename/move selected mismatches.


