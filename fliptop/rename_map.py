"""
fliptop.rename_map

Canonical emcee name mapping.

This file contains the alias to canonical name mapping that is used to
clean up emcee names in the data cleaning pipeline.

Populate RENAME_MAP with the same contents as your `rename_dict`
from the old data_cleaning.ipynb notebook.
"""

from __future__ import annotations

from typing import Mapping

# Alias to canonical name mapping.
# Example:
# RENAME_MAP = {
#     "Looniee": "Loonie",
#     "Anygma (host)": "Anygma",
# }
#
# Replace the example with the full mapping from your notebook.
RENAME_MAP: Mapping[str, str] = {
    "Daddie Joe D": "Daddy Joe D",
    "DaddyJoe D": "Daddy Joe D",
    "D.O.C. Pau": "Doc Pau",
    "DOC Pau": "Doc Pau",
    "Damnsa": "Damsa",
    "Pareng Elbiz": "Elbiz",
    "Flip": "Flipzydot1",
    "Frooztreitted Hoemmizyd": "Frooz",
    "GusTav": "Gustav",
    "Hallucinate": "Kris Delano",
    "Ice Rocks": "Saint Ice",
    "J Skeelz": "J-Skeelz",
    "Japormz": "Jhapormz",
    "JayTee": "Jaytee",
    "joshG": "Josh G",
    "JoshG": "Josh G",
    "Juan Lazy": "Juan Tamad",
    "Hearty Tha Bomb": "Hearty",
    "Mac-T": "Mac T",
    "Malupet": "Malupit",
    "Marshall": "Marshall Bonifacio",
    "Mel Christ": "Melchrist",
    "MelChrist": "Melchrist",
    "Nerdskillz": "Nerd Skillz",
    "One3D": "One3d",
    "Poison 13": "Poison13",
    "B.I.L.L.Y.": "Prosecutor Billy",
    "R Zone": "R-Zone",
    "RanieBoy": "Ranieboy",
    "Righteous-One": "Righteous One", 
    "Righteous1": "Righteous One",
    "Single Shot": "SingleShot",
    "Spade": "Goriong Talas",
    "Stiffler": "Stiff",
    "Tim aka Cleave Heckler": "Tim",
    "W-Beat": "W Beat",
    "WBeat": "W Beat",
    "Young One": "YoungOne",
    "2Khelle": "2khelle",
    "Akt": "AKT",
    "Crhyme": "CRhyme",
    "Markong Bungo": "Poison13",
    "Freak Sanchez": "Tipsy D",
    "Ghostly": "Goriong Talas",
    "No. 144": "Emar Industriya",
    "Carlito": "Sayadd",
    "sKarm": "Skarm",
    "Cripli": "CripLi",
    "M-Zhayt": "M Zhayt",
    "Jdee": "JDee",
    "Mastafeat": "MastaFeat",
    "Cnine": "CNine",
    "Sinagtala": "GL",
    "1ce Water": "J-Blaque",
    "Deadpan": "Shehyee",
    "Mia Sonin": "Batang Rebelde",
    "GameBoy": "Gameboy",
    "Lil Strocks": "LilStrocks",
    "Lil John": "LilJohn",
    "NIkki": "Nikki",
    "Nico": "AKT"
}