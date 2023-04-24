# This is a telegram bot to automate and control the
# periodic updates for the two databases.

"""
status - Checks if bot is alive.
begin - Starts the recurring update job. Run once, after every restart.
update - Forces an update of the database right now.
download - Downloads either or both the databases.
"""


from bulletindatabase import BulletinDatabase
from tledatabase import TleDatabase

import common_bot_interfaces as cbi

import datetime as dt
import sys

from telegram.ext import CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

class TleBulletinInterface:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Placeholder
        self.updateJob = None
        self.updateFrequency = 7200.0 # How often to update in seconds

        # We fix the filepaths, not much point making it configurable
        self.tledbpath = "tles.db"
        self.bulletindbpath = "bulletins.db"
        self.tledb = TleDatabase(self.tledbpath)
        self.bulletindb = BulletinDatabase(self.bulletindbpath)

    def _addInterfaceHandlers(self):
        super()._addInterfaceHandlers()

        # Add handlers down here..
        print("Adding TleBulletinInterface:begin")
        self._app.add_handler(CommandHandler(
            "begin",
            self.begin,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:update")
        self._app.add_handler(CommandHandler(
            "update",
            self.update,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:download")
        self._app.add_handler(CommandHandler(
            "download",
            self.download,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:_downloadResponse")
        self._app.add_handler(MessageHandler(
            self.ufilts & filters.Regex("Download"),
            self._downloadResponse
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

    ##########################
    async def download(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Provides options to download either or both databases.
        """

        kb = ReplyKeyboardMarkup(
            [
                ["Download TLEs", "Download Bulletins"],
                ["Download Both"]
            ],
            one_time_keyboard=True
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Select an option:",
            reply_markup=kb
        )

    async def _downloadResponse(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Responds to the download command.
        """
        if update.message.text == "Download TLEs":
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(self.tledbpath, "rb")
            )
        elif update.message.text == "Download Bulletins":
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(self.bulletindbpath, "rb")
            )
        elif update.message.text == "Download Both":
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(self.tledbpath, "rb")
            )
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(self.bulletindbpath, "rb")
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
