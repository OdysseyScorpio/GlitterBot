from collections import Counter

from lib.database import Database
from lib.gwpcc.consts import KEY_THING_INDEX, KEY_THING_LOCALE_KNOWN_LANGUAGES, KEY_THING_LOCALE_THING_NAMES, \
    KEY_THING_LOCALE_THING_NAME, KEY_THING_LOCALE_FULL_TEXT_INDEX
# The index only updates english names
from lib.gwpcc.things.thing import Thing
from lib.log import Logger


def update():
    db = Database().connection
    # Load the thing index
    thing_index = db.smembers(KEY_THING_INDEX)

    # Get list of known languages
    languages = db.smembers(KEY_THING_LOCALE_KNOWN_LANGUAGES)

    pipe = db.pipeline()

    # Erase existing indices
    fti_keys = [key for key in db.scan_iter(KEY_THING_LOCALE_FULL_TEXT_INDEX.format('*'), count=100000)]
    if fti_keys:
        pipe.delete(*fti_keys)
        pipe.execute()

    _build_thing_def_index(list(thing_index), db)

    # For each language
    for language in languages:

        Logger().log.debug('Updating thing name index for {}'.format(language))

        # For each thing, get the highest scoring localised name proposed
        name_pipe = db.pipeline()
        for thing_hash in thing_index:
            name_pipe.zrevrange(KEY_THING_LOCALE_THING_NAMES.format(language, thing_hash), 0, 0, withscores=True)
        results = name_pipe.execute()
        hash_key_proposed_names = dict(zip(thing_index, results))

        # For each thing, get the current localised name
        name_pipe = db.pipeline()
        for thing_hash in thing_index:
            name_pipe.get(KEY_THING_LOCALE_THING_NAME.format(language, thing_hash))
        results = name_pipe.execute()
        hash_key_current_names = dict(zip(thing_index, results))

        # For each thing
        for thing_hash in thing_index:

            proposed_names = hash_key_proposed_names.get(thing_hash)

            # Get the existing name
            name = hash_key_current_names.get(thing_hash)

            # Check if we had a new name to compare
            if proposed_names:

                # Unpack name,score tuple
                new_name, score = proposed_names[0]

                # Is there one where it has 50 votes?
                if score >= 2:

                    # Is the proposed name still the same as the current name if any?
                    if name != new_name:
                        name = new_name
                        Logger().log.debug(
                            'New name for {} accepted, will become {} ({} votes)'.format(thing_hash, name, score))

                        # Set the new name
                        pipe.set(KEY_THING_LOCALE_THING_NAME.format(language, thing_hash), name)
                else:
                    Logger().log.debug('New name for {} rejected ({} votes)'.format(thing_hash, score))

            # Add name to the index if one can be set
            if name:
                _update_index(pipe, name, thing_hash)

    # Execute
    pipe.execute()


def _update_index(pipeline, string: str, thing_hash: str):
    # Now split the new name and update the indices
    letters = [c.lower() for c in string if c.isalnum()]

    # Count how many times a letter occurs in the word
    scores = Counter(letters)

    for letter, score in scores.items():
        pipeline.zincrby(KEY_THING_LOCALE_FULL_TEXT_INDEX.format(letter), score, thing_hash)


def _build_thing_def_index(thing_index, connection):
    pipe = connection.pipeline()
    # Process things, 1000 at a time so as not to murder memory.
    for chunk in chunks(thing_index, 1000):
        things = Thing.get_many_from_database_by_hash_without_history(chunk, connection)

        for thing_hash, thing in things.items():
            if thing is None:
                Logger().log.error('Found {} with no metadata!'.format(thing_hash))
                continue
            _update_index(pipe, thing.FullName, thing.Hash)
    pipe.execute()


def chunks(list_, n):
    """Yield successive n-sized chunks from list_."""
    for i in range(0, len(list_), n):
        yield list_[i:i + n]
