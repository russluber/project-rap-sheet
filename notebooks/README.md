# Living Document

# Research Question
----

The central question of this project revolves around FlipTop battle rap careers and their length.

> How long does a career in the Philippines' premier rap battle league FlipTop last?

Who rises to the top? What determines who stays relevant over the years?

Here I'm thinking survival (time-to-event) analysis.

Another question I'm curious about is this:
> Who has battled who? And what matchups haven't been done yet?

I'm picturing a graph here. I want to understand how dense or sparse the network of rap battles are in FlipTop.

Ideas:
- Emcees are nodes
- "Battled" as edges
- Weight of the edge is how many times they've battled?


While the majority of the videos posted on the [FlipTop YouTube channel](https://www.youtube.com/@fliptopbattles) are rap battles, a number of them are video flyers, announcement videos, behind-the-scenes content, reaction videos, and the like. As such, it's important to filter out these videos so that we're left to analyze the data from the **rap battles** alone.

### But what constitutes a **rap battle**?

Good question.

My criteria for what is considered a rap battle for the purposes of this project:
1. The video needs to be *a capella* (no underlying beat to accompany emcees rapping).
2. The video involves emcees performing written material (not all of their rounds are off-the-top freestyle). 
    - The early days of FlipTop saw emcees testing each other's skills in the artform known as off-the-top freestyle, where emcees would take turns berating each other lyrically with material they thought of on the spot or in the moment of speaking. 
3. To a lesser extent, the video needs to involve judging at the end (there needs to be stakes).

Note:
- **By these criteria, earlier videos of FlipTop wouldn't be included. Need explanation for what I'm doing here.**
    - I've changed my mind on this. I've included them alongside the freestyle battles whether a capella or with a beat.



### Keywords for Exclusion
As a long-time viewer of these videos, there's a couple key words that makes filtering with these criteria in mind easier. 
- Include:
    - **vs** - most, if not all, of the a capella rap battles in the FlipTop YouTube channel have "vs" in the video title. 
        - For the uninitiated: "vs" is short for "versus." 
- Exclude:
    - **tryout** - while these are battles, they are for the newcomers to the scene and is often not judged, especially in older videos.
    - **beatbox** - this is another genre of battle separate from the a capella, judged battles.
    - **flyer** and **promo** - these are advertisements and announcement videos for upcoming events.
    - **Anygma Machine** - Anygma, the head of FlipTop as a company, sometimes reviews battles and gives his take on upcoming matches.
        - A reference to the real [Enigma Machine](https://en.wikipedia.org/wiki/Enigma_machine) that the allies had to break in WW2.
   - **[LIVE]** - live performances from the FlipTop Festival event that happened in 2020.
   - **UnggoYan** - Emcees read comments left on videos of their previous battles
   - **Pre-Battle Interviews** - self-explanatory
   - **Salitang Ugat** - translation: "root word." These are interviews of notable emcees who tell the stories behind how they came up with their rap battle name.
   - **Trailer** - promo video trailer for upcomming events
   - **Video Flyer** - self-explanatory
   - **Silip** - BTS videos added recently
   - **Sound Check** - Pre-event check in with FlipTop event prep stuff
   - **Tribute** - tribute to dead rappers
   - **Tutok** - other BTS videos?
   - **Abangan** - clips

For the scope of this project, I will only consider the battles that are between two people. FlipTop has a variety of rap battle formats, not just two people insulting each other back and forth. Examples include: the Royal Rumble, the 5 vs 5, and the tag-team 2 vs 2 (Dos Por Dos) matches. The vast majority of the battles, though, are one versus one. Those battles will be the focus of this project.



### Dealing with Aliases
What to do about well-known emcees that have battled under aliases (not their usually emcee names)?
- Poison13 as Markong Bungo
- Tipsy D as Freak Sanchez
- Goriong Talas as Ghostly
- Emar Industriya as No. 144
- Sayadd as Carlito

I've decided to do away with these gimmick aliases. At the end of the day, what I'm interested in is the careers of the *people* behind the personas. I will count these battles as battles under their main emcee names. Not the aliases.

An outline of how I standardized the names:
- I took each emcee's most recent name if they had more than one spelling of their name or if they renamed themselves over time.
- Aesthetically speaking, I arbitrarily picked whichever name looks more appealing to me e.g. "Daddy Joe D" vs "Daddie Joe D" vs "DaddieJoe D"
- I cross-referenced less well-known emcees who have changed their names over the years by comparing faces across the videos.
- `rename_dict`



### Actual Event Dates -- ONGOING task
- Go back to before all this analysis and extract dates for actual event dates of the battles.
- Hard task.
- But also: The battle being uploaded to YouTube is *part* of the battlers' careers. It's like their music videos releasing after the audio has been released months before. So technically maybe we don't *need* to do all that very tricky text extraction and NLP.

All COVID-era battles don't have `event_date`s to them.
- There are also unreasonable value for `event_date`s in `df_meta` that need to be cleaned up. The rows with "June XX, XXXX" in them.
- Let's first deal with the NAs. This will be a difficult thing to do.
- In particular any battle occuring after The FlipTop Festival and until Ahon 12 (Quarantine Isabuhay 2020 and 2021).

Events that I need to find actual dates for:
- Ahon 11 (Day 2)
- Ahon 11 (Day 1)
- Bwelta Balentong 7 (Day 2)
- Bwelta Balentong 7 (Day 1)
- Zoning 10
- Unibersikulo 8
- Second Sight 8
- Ahon 12 (Day 2)
- Ahon 12 (Day 1)
- Unibersikulo 10
- Zoning 13
- Unibersikulo 9
- Grain Assault 11
- Zoning 11 
- Zoning 12
- Bwelta Balentong 8

Second Sight 8 was the first event that happened during the COVID era. The last event in the COVID era was Ahon 12. Info here:
- https://www.fliptop.com.ph/articles/an-unforgettable-second-sight-8


### Further-down-the-line questions
- One on one career statistics for each battler?
- Make the project webpage? Graph explorer via D3.
- Need more survival analysis stuff to try and find out.

Some questions I'm interested in:
- Which emcees have had the biggest comebacks?
- Basic stuff like most viewed battler.
- Correlation between length of career and views over time? Do emcees get popular over time or something?
- Winstreaks? Maybe I could manually go through each video and add another col to who won? Who has the longest winstreak?



### Actual Event/Venue Location information -- task for later
- Need to clean up

