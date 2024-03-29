# This is a telegram bot to automate and control the
# periodic updates for the two databases.

"""
status - Checks if bot is alive.
begin - Starts the recurring update job. Run once, after every restart.
update - Forces an update of the database right now.
download - Downloads either or both the databases. Optional: (starttime) (stoptime).
add - Adds a TLE table to the download selection.
selection - Views your current TLE download selection.
clear - Clears your download selection.
"""


from bulletindatabase import BulletinDatabase
from tledatabase import TleDatabase

import common_bot_interfaces as cbi

import datetime as dt
import sys
import os
import pickle

from telegram.ext import CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
import telegram

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

        # Container to hold user download tables
        self.downloadTablesPicklePath = "UserDownloadTables.pkl"
        if os.path.exists(self.downloadTablesPicklePath):
            with open(self.downloadTablesPicklePath, "rb") as f:
                self.downloadTables = pickle.load(f)
        else:
            self.downloadTables = dict()
        print(self.downloadTables)

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
        # print("Adding TleBulletinInterface:_downloadResponse")
        # self._app.add_handler(MessageHandler(
        #     self.ufilts & filters.Regex("Download"),
        #     self._downloadResponse
        # ))
        print("Adding TleBulletinInterface:selection")
        self._app.add_handler(CommandHandler(
            "selection",
            self.selection,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:add")
        self._app.add_handler(CommandHandler(
            "add",
            self.add,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:clear")
        self._app.add_handler(CommandHandler(
            "clear",
            self.clear,
            filters=self.ufilts
        ))
        print("Adding TleBulletinInterface:sources")
        self._app.add_handler(CommandHandler(
            "sources",
            self.sources,
            filters=self.ufilts & self._adminfilter # Provided by other interfaces in the class definition at the end
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
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Please wait while I update the databases."
        )

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
        # Show error message if number of args is wrong
        if len(context.args) > 2:
             await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Invalid number of arguments. Calling args are:\n" + 
                "/download (optional: start time) (optional: stop time)\n" + 
                "Example: /download 1672800000 1672900000"
            )

        # Send TLE db
        try:
            await self._downloadUserTles(update, context)
        except telegram.error.TimedOut:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Timed out while uploading your TLE database. Try specifying a smaller time window."
            )
        # Send bulletin db
        try:
            await self._downloadUserBulletins(update, context)
        except telegram.error.TimedOut:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Timed out while uploading your bulletin database. Try specifying a smaller time window."
            )

    async def _downloadUserTles(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # If no selection yet, then tell the user
        usertables = self.downloadTables.get(update.effective_user.id)
        if usertables is None or len(usertables) == 0:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="You have no selected TLE tables for download. Add them with /add."
            )

        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Please wait while I prepare your selection."
            )

            # Create a db for this user on disk
            userdbpath = "tles_%d.db" % (update.effective_user.id)
            userdb = TleDatabase(userdbpath)

            # Create the user's tables in the db
            for tablename in usertables:
                userdb.makeSatelliteTable(None, tablename)
            userdb.close() # We don't need it to be open any more

            # Attach the user db for inserts
            if len(context.args) <= 2:
                self.tledb.execute(
                    "ATTACH DATABASE '%s' AS userdb" % (userdbpath)
                )

            try:
                # Insert the user's tables' rows into the new db in full
                if len(context.args) == 0:
                    for tablename in usertables:
                        self.tledb.execute(
                            "INSERT INTO userdb.'%s' SELECT * FROM '%s'" % (tablename, tablename)
                        )
                    self.tledb.commit()

                # Or insert rows starting from a certain time
                elif len(context.args) == 1:
                    for tablename in usertables:
                        self.tledb.execute(
                            "INSERT INTO userdb.'%s' SELECT * FROM '%s' WHERE time_retrieved > ?" % (tablename, tablename),
                            (float(context.args[0]),)
                        )
                    self.tledb.commit()

                # Or insert rows starting from a certain time and stopping at a certain time
                elif len(context.args) == 2:
                    for tablename in usertables:
                        self.tledb.execute(
                            "INSERT INTO userdb.'%s' SELECT * FROM '%s' WHERE time_retrieved > ? AND time_retrieved < ?" % (tablename, tablename),
                            (float(context.args[0]), float(context.args[1]))
                        )
                    self.tledb.commit()

                # We don't need a separate message here for showing the calling structure

            except sq.IntegrityError as e: # We must catch this so that the detach database proceeds
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="There was an error while creating your TLE database:\n" + str(e)
                )


            # Cleanup and Delete the user db from disk
            if len(context.args) <= 2:
                self.tledb.execute(
                    "DETACH DATABASE userdb"
                )
                # Send the newly created user db
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=open(userdbpath, "rb"),
                    write_timeout=60 # Have a longer timeout
                )
            os.remove(userdbpath)

    async def _downloadUserBulletins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # For bulletins, we just download according to the time selection, if specified
        if len(context.args) == 0:
            # Send the full database
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(self.bulletindbpath, "rb"),
                write_timeout=60 # Have a longer timeout
            )
        elif len(context.args) <= 2:
            # Extract from the arguments
            start = float(context.args[0]) if len(context.args) >= 1 else None
            end = float(context.args[1]) if len(context.args) >= 2 else None

            # Create a new database
            userbulletindbpath = "bulletins_%d.db" % (update.effective_user.id)
            userbulletinsdb = BulletinDatabase(userbulletindbpath)

            # Recreate the same tables in the current database
            for src in self.bulletindb.tablenames:
                userbulletinsdb.makeBulletinTable(src)
            userbulletinsdb.close() # We don't need it to be open any more
            # Then attach the user db to the current one
            self.bulletindb.execute(
                "ATTACH DATABASE '%s' AS userbulletinsdb" % (userbulletindbpath)
            )

            for src in self.bulletindb.tablenames:
                if start is not None:
                    if end is not None:
                        # Slice between the two timings and insert
                        self.bulletindb.execute(
                            "INSERT INTO userbulletinsdb.'%s' SELECT * FROM '%s' WHERE time_retrieved >? AND time_retrieved <?" % (src, src),
                            (start, end)
                        )
                    else:
                        # Slice from the start time and insert
                        self.bulletindb.execute(
                            "INSERT INTO userbulletinsdb.'%s' SELECT * FROM '%s' WHERE time_retrieved >?" % (src, src),
                            (start,)
                        )
                
            self.bulletindb.commit()
            self.bulletindb.execute(
                "DETACH DATABASE userbulletinsdb"
            )
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=open(userbulletindbpath, "rb"),
                write_timeout=60 # Have a longer timeout
            )

            # Delete the user bulletins db from disk
            os.remove(userbulletindbpath)

        # We don't need a separate message here for showing the calling structure


    ##########################
    async def selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Command to display user selected tables for download.
        """
        userTables = self.downloadTables.get(update.effective_user.id)
        if userTables is None or len(userTables) == 0:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="You have no tables in your download list."
            )
        else:
            s = "\n".join(userTables)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="These are your selected tables for downloads: \n\n" + s
            )

    ##########################
    async def add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Command for the user to add a table to his/her download list.
        """
        if len(context.args) == 0:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Please specify a satellite name."
            )
        else:
            await self._addUserTable(update, context)

    async def _addUserTable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Check if the tablename exists
        userid = update.effective_user.id
        sats = self.tledb.tablenames
        tablename = " ".join(context.args)
        print("%d asked for: %s" % (userid, tablename))

        found = False
        for sat in sats:
            if tablename in sat:
                # Add the table to the set for this user
                if userid not in self.downloadTables:
                    self.downloadTables[userid] = set()
                self.downloadTables[userid].add(sat)

                # Dump to file for caching user selections
                with open(self.downloadTablesPicklePath, "wb") as f:
                    pickle.dump(self.downloadTables, f)

                # Update the user
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Okay, I have added %s to your download list." % sat
                )
                found = True

        if not found:
            # Just do a coarse contains for now; we do fuzzy matching later
            closeMatches = [sat for sat in sats if tablename.lower() in sat.lower()]
            print(closeMatches)
            print(sats)

            # Update the user
            if len(closeMatches) > 0:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Sorry, I don't know about that satellite. " + 
                        "Maybe you were looking for one of the following?\n\n" 
                        + "\n".join(closeMatches)
                )
            else:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Sorry, I don't know about that satellite. Check your spelling?"
                )

    ##########################
    async def clear(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Clears the user's download selection.
        """
        if self.downloadTables[update.effective_user.id] is not None:
            self.downloadTables[update.effective_user.id].clear()
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Your download list has been cleared."
        )

        # Dump to file for caching user selections
        with open(self.downloadTablesPicklePath, "wb") as f:
            pickle.dump(self.downloadTables, f)

    ##########################
    async def sources(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if len(context.args) == 0:
            # Show the available set of sources
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="These are the available sources:\n" + "\n".join(
                    [key for key in self.tledb.getAvailableSrcs()]
                )
            )

        else:
            # Replace with specified sources
            self.tledb.setSrcs(context.args)

        # No matter what, show what is used now
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="These are your used sources now:\n" + "\n".join(self.tledb.usedSrcs)
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
    if len(sys.argv) < 2:
        print("Usage: python bot.py <admin_id>")
        print("If using bot runner, then: python -m common_bot_interfaces.bot_runner bot.py <admin_id>")
        print("Place your bot token in the environment variable TLEBULLETINBOT_TOKEN.")
        sys.exit(1)

    bot = TleBulletinBot.fromEnvVar('TLEBULLETINBOT_TOKEN')
    bot.setAdmin(int(sys.argv[1]))
    # Configure the databases
    bot.tledb.setSrcs(["geo"])
    bot.bulletindb.setSrcs(["dailyiau2000", "dailyiau1980"])
    # Run
    bot.run()
