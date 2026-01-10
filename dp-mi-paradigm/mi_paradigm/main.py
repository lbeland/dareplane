from fire import Fire
import time
import random

from mi_paradigm.utils.logging import logger

logger.setLevel(10)

# timing parameters
t_pre = 1
t_show = 1
t_post = 1


# LSL outlet - for convenience we also display to the logger
class Outlet:
    def __init__(self):
        self.logger = logger

    def push_sample(self, sample: str):
        self.logger.debug(f"Pushing sample {sample}")
        # Simulate LSL outlet without pylsl dependency
        print(f"[LSL] {sample}")

# Sometimes is is more convenient to have a paradigm instance which can be
# kept alive globally. This especially holds if the server we will wrap around
# this module will not call psychopy in a subprocess
# --> So for the example, add a class
class Paradigm:
    def __init__(self):
        self.open_window()

    def open_window(self):
        # Simulated window without psychopy
        print("Opening window (simulated)")
        self.win = {"width": 800, "height": 600}
        self.rstim = "R"
        self.lstim = "L"
        self.fix_cross = "+"

    def close_window(self):
        print("Closing window (simulated)")


def run_mi_task(paradigm: Paradigm, nrepetitions: int = 4) -> int:
    outlet = Outlet()

    # Show fixation cross
    print(f"Displaying: {paradigm.fix_cross}")

    # create a balanced set
    directions = ["R"] * (nrepetitions // 2) + ["L"] * (nrepetitions // 2)
    random.shuffle(directions)

    for i, dir in enumerate(directions):
        print(f"\n--- Trial {i+1}/{nrepetitions} ---")
        print(f"Displaying: {paradigm.fix_cross}")
        outlet.push_sample("new_trial")

        time.sleep(t_pre)

        # Show direction stimulus
        if dir == "R":
            stim = paradigm.rstim
        else:
            stim = paradigm.lstim

        print(f"Displaying: {stim}")
        outlet.push_sample(dir)

        # clear screen and sleep for post
        time.sleep(t_show)
        print("Screen cleared")
        outlet.push_sample("cleared")

        time.sleep(t_post)

    return 0


if __name__ == "__main__":

    from functools import partial
    pdm = Paradigm()
    Fire(partial(run_mi_task, pdm))