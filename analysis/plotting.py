from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


def save_measurement_plot(dataframe: pd.DataFrame, file_path: Path, title: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8, 4.8))
    axis.plot(dataframe["potential_v"], dataframe["current_a"], linewidth=1.2)
    axis.set_xlabel("potential_v")
    axis.set_ylabel("current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)


def save_mean_curve_plot(dataframe: pd.DataFrame, file_path: Path, title: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8, 4.8))
    axis.plot(dataframe["potential_v"], dataframe["mean_current_a"], label="mean_current_a", linewidth=1.4)
    axis.fill_between(
        dataframe["potential_v"],
        dataframe["mean_current_a"] - dataframe["std_current_a"],
        dataframe["mean_current_a"] + dataframe["std_current_a"],
        alpha=0.2,
        label="±SD",
    )
    axis.set_xlabel("potential_v")
    axis.set_ylabel("mean_current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend()
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)
