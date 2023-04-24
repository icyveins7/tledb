# This is a telegram bot to automate and control the
# periodic updates for the two databases.

from bulletindatabase import BulletinDatabase
from tledatabase import TleDatabase

import common_bot_interfaces as cbi

import datetime as dt
import sys

from telegram.ext import CommandHandler, ContextTypes, CallbackQueryHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup

class TleBulletinInterface:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Placeholder
        self.updateJob = None
        self.updateFrequency = 7200.0 # How often to update in seconds

        # We fix the filepaths, not much point making it configurable
        self.tledb = TleDatabase("tles.db")
        self.bulletindb = BulletinDatabase("bulletins.db")

    def _addInterfaceHandlers(self):
        super()._addInterfaceHandlers()

        # Add handlers down here..
        print("Adding TleBulletinInterface:begin")
        self._app.add_handler(CommandHandler(
            "begin",
            self.begin,
            filters=self.ufilts & self._adminfilter
        ))
        print("Adding TleBulletinInterface:update")
        self._app.add_handler(CommandHandler(
            "update",
            self.update,
            filters=self.ufilts & self._adminfilter
        ))

    ##########################
    async def begin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Check if the job has already begun?
        if self.updateJob is not None:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="The update job has already begun. I will update every %d seconds." % (
                    self.updateFrequency)
            )

        # Otherwise begin the job with the specified frequency
        else:
            if len(context.args) > 0:
                self.updateFrequency = int(context.args[0])
            
            # Start the job and recur forever
            self.updateJob = context.job_queue.run_repeating(
                self._update, self.updateFrequency, first=1.0,
                data=update.effective_chat.id
            )
            # For some reason when first=0.0 seconds it bugs out, so leave it at 1.0

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Okay, I just began the update job. I will update every %d seconds." % (
                    self.updateFrequency)
            )

    ##########################
    async def update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Force an update right now
        self.tledb.update()
        self.bulletindb.update()

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Okay, I just updated the databases. This will not affect my recurring updates."
        )

    async def _update(self, context: ContextTypes.DEFAULT_TYPE):
        """
        Recurring job to update both databases.
        """
        print("Starting database updates...")

        # Update databases
        self.tledb.update()
        self.bulletindb.update()

        await context.bot.send_message(
            chat_id = context.job.data,
            text = "Okay, I just updated the databases. This was a part of my recurring updates."
        )



#%% #################################
class TleBulletinBot(
    TleBulletinInterface,
    cbi.GitInterface, 
    cbi.ControlInterface,
    cbi.StatusInterface,
    cbi.BotContainer
):
    pass

#%% ##################################
if __name__ == "__main__":
    bot = TleBulletinBot.fromEnvVar('TLEBULLETINBOT_TOKEN')
    bot.setAdmin(int(sys.argv[1]))
    # Configure the databases
    bot.tledb.setSrcs(["geo"])
    bot.bulletindb.setSrcs(["dailyiau2000", "dailyiau1980"])
    # Run
    bot.run()
