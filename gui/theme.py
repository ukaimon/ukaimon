from __future__ import annotations

import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk


SURFACE_BG = "#f4f6f8"
CARD_BG = "#ffffff"
CARD_ALT_BG = "#eef4f7"
BORDER = "#c8d3dc"
TEXT = "#163042"
MUTED_TEXT = "#587182"
ACCENT = "#0f6c74"
ACCENT_SOFT = "#dff0f1"
WARNING = "#8b0000"
SELECT_BG = "#c7e8ea"
SELECT_TEXT = "#0f2430"


def _configure_fonts(root: tk.Tk) -> None:
    default_font = tkfont.nametofont("TkDefaultFont")
    default_font.configure(family="Yu Gothic UI", size=11)

    text_font = tkfont.nametofont("TkTextFont")
    text_font.configure(family="Yu Gothic UI", size=11)

    heading_font = tkfont.nametofont("TkHeadingFont")
    heading_font.configure(family="Yu Gothic UI", size=11, weight="bold")

    menu_font = tkfont.nametofont("TkMenuFont")
    menu_font.configure(family="Yu Gothic UI", size=11)

    caption_font = tkfont.nametofont("TkCaptionFont")
    caption_font.configure(family="Yu Gothic UI", size=11)

    root.option_add("*Font", "TkDefaultFont")
    root.option_add("*Listbox.font", "TkTextFont")
    root.option_add("*Listbox.background", CARD_BG)
    root.option_add("*Listbox.foreground", TEXT)
    root.option_add("*Listbox.selectBackground", SELECT_BG)
    root.option_add("*Listbox.selectForeground", SELECT_TEXT)
    root.option_add("*Listbox.activestyle", "none")
    root.option_add("*Listbox.borderWidth", 1)
    root.option_add("*Listbox.relief", "solid")
    root.option_add("*Listbox.highlightThickness", 0)
    root.option_add("*Listbox.selectBorderWidth", 0)
    root.option_add("*Menu.font", "TkMenuFont")


def apply_app_theme(root: tk.Tk) -> ttk.Style:
    _configure_fonts(root)
    root.configure(background=SURFACE_BG)
    root.option_add("*tearOff", False)

    style = ttk.Style(root)
    if "clam" in style.theme_names():
        style.theme_use("clam")

    style.configure(".", background=SURFACE_BG, foreground=TEXT)
    style.configure("TFrame", background=SURFACE_BG)
    style.configure("Card.TFrame", background=CARD_BG)
    style.configure("AltCard.TFrame", background=CARD_ALT_BG)

    style.configure("TLabel", background=SURFACE_BG, foreground=TEXT, padding=(0, 1))
    style.configure("Subtle.TLabel", background=SURFACE_BG, foreground=MUTED_TEXT)
    style.configure("Title.TLabel", background=SURFACE_BG, foreground=TEXT, font=("Yu Gothic UI", 18, "bold"))
    style.configure("Section.TLabel", background=SURFACE_BG, foreground=TEXT, font=("Yu Gothic UI", 12, "bold"))
    style.configure("Emphasis.TLabel", background=SURFACE_BG, foreground=WARNING, font=("Yu Gothic UI", 16, "bold"))
    style.configure("CardTitle.TLabel", background=CARD_BG, foreground=TEXT, font=("Yu Gothic UI", 12, "bold"))
    style.configure("CardBody.TLabel", background=CARD_BG, foreground=TEXT)
    style.configure("CardSubtle.TLabel", background=CARD_BG, foreground=MUTED_TEXT)

    style.configure(
        "TButton",
        padding=(12, 8),
        relief="flat",
        borderwidth=1,
        background=CARD_BG,
        foreground=TEXT,
    )
    style.map(
        "TButton",
        background=[("pressed", ACCENT_SOFT), ("active", ACCENT_SOFT)],
        foreground=[("disabled", MUTED_TEXT)],
        bordercolor=[("focus", ACCENT)],
    )

    style.configure(
        "TEntry",
        fieldbackground=CARD_BG,
        foreground=TEXT,
        bordercolor=BORDER,
        lightcolor=BORDER,
        darkcolor=BORDER,
        insertcolor=TEXT,
        padding=(8, 6),
    )
    style.configure(
        "TCombobox",
        fieldbackground=CARD_BG,
        foreground=TEXT,
        bordercolor=BORDER,
        lightcolor=BORDER,
        darkcolor=BORDER,
        arrowsize=16,
        padding=(8, 6),
    )
    style.map(
        "TCombobox",
        fieldbackground=[("readonly", CARD_BG)],
        selectbackground=[("readonly", CARD_BG)],
        selectforeground=[("readonly", TEXT)],
    )

    style.configure(
        "TLabelframe",
        background=SURFACE_BG,
        borderwidth=1,
        relief="solid",
        bordercolor=BORDER,
        padding=(14, 12),
    )
    style.configure(
        "TLabelframe.Label",
        background=SURFACE_BG,
        foreground=TEXT,
        font=("Yu Gothic UI", 12, "bold"),
        padding=(4, 0),
    )

    style.configure("TNotebook", background=SURFACE_BG, borderwidth=0, tabmargins=(6, 6, 6, 0))
    style.configure(
        "TNotebook.Tab",
        background="#dde6ec",
        foreground=TEXT,
        padding=(18, 10),
        font=("Yu Gothic UI", 11, "bold"),
    )
    style.map(
        "TNotebook.Tab",
        background=[("selected", CARD_BG), ("active", ACCENT_SOFT)],
        foreground=[("selected", ACCENT), ("active", TEXT)],
    )

    style.configure(
        "Treeview",
        background=CARD_BG,
        fieldbackground=CARD_BG,
        foreground=TEXT,
        bordercolor=BORDER,
        rowheight=30,
        relief="flat",
    )
    style.map(
        "Treeview",
        background=[("selected", SELECT_BG)],
        foreground=[("selected", SELECT_TEXT)],
    )
    style.configure(
        "Treeview.Heading",
        background="#dde6ec",
        foreground=TEXT,
        relief="flat",
        borderwidth=0,
        padding=(10, 8),
        font=("Yu Gothic UI", 11, "bold"),
    )
    style.map("Treeview.Heading", background=[("active", ACCENT_SOFT)])

    style.configure("Vertical.TScrollbar", background="#dde6ec", troughcolor=SURFACE_BG, bordercolor=SURFACE_BG)
    style.configure("Horizontal.TScrollbar", background="#dde6ec", troughcolor=SURFACE_BG, bordercolor=SURFACE_BG)

    return style
