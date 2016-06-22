#!/usr/bin/env python2
# coding=utf-8

from __future__ import unicode_literals

from datetime import date, datetime, timedelta
import os
import sys

from libs.six import text_type

import core
from core import logger, nzbToMediaDB
from core.nzbToMediaUtil import convert_to_ascii, CharReplace, plex_update, replace_links
from core.nzbToMediaUserScript import external_script


def process_torrent(directory, name, category, torrent_hash, torrent_id, agent):
    status = 1  # 1 = failed | 0 = success
    root = 0
    found_file = 0

    if agent != 'manual' and not core.DOWNLOADINFO:
        logger.debug('Adding TORRENT download info for directory {0} to database'.format(directory))

        database = nzbToMediaDB.DBConnection()

        directory_1 = directory
        name_1 = name

        try:
            encoded, directory_1 = CharReplace(directory)
            encoded, name_1 = CharReplace(name)
        except:
            pass

        control_value_dict = {
            'input_directory': text_type(directory_1)
        }
        new_value_dict = {
            'input_name': text_type(name_1),
            'input_hash': text_type(torrent_hash),
            'input_id': text_type(torrent_id),
            'client_agent': text_type(agent),
            'status': 0,
            'last_update': date.today().toordinal()
        }
        database.upsert('downloads', new_value_dict, control_value_dict)

    logger.debug('Received Directory: {0} | Name: {1} | Category: {2}'.format(directory, name, category))

    # Confirm the category by parsing directory structure
    directory, name, category, root = core.category_search(directory, name, category,
                                                           root, core.CATEGORIES)
    if category == '':
        category = 'UNCAT'

    usercat = category
    try:
        name = name.encode(core.SYS_ENCODING)
    except UnicodeError:
        pass
    try:
        directory = directory.encode(core.SYS_ENCODING)
    except UnicodeError:
        pass

    logger.debug('Determined Directory: {0} | Name: {1} | Category: {2}'.format
                 (directory, name, category))

    # auto-detect section
    section = core.CFG.findsection(category).isenabled()
    if section is None:
        section = core.CFG.findsection('ALL').isenabled()
        if section is None:
            logger.error('Category:[{0}] is not defined or is not enabled. '
                         'Please rename it or ensure it is enabled for the appropriate section '
                         'in your autoProcessMedia.cfg and try again.'.format
                         (category))
            return [-1, '']
        else:
            usercat = 'ALL'

    if len(section) > 1:
        logger.error('Category:[{0}] is not unique, {1} are using it. '
                     'Please rename it or disable all other sections using the same category name '
                     'in your autoProcessMedia.cfg and try again.'.format
                     (usercat, section.keys()))
        return [-1, '']

    if section:
        section_name = section.keys()[0]
        logger.info('Auto-detected SECTION:{0}'.format(section_name))
    else:
        logger.error('Unable to locate a section with subsection:{0} '
                     'enabled in your autoProcessMedia.cfg, exiting!'.format
                     (category))
        return [-1, '']

    section = dict(section[section_name][usercat])  # Type cast to dict() to allow effective usage of .get()

    torrent_no_link = int(section.get('Torrent_NoLink', 0))
    keep_archive = int(section.get('keep_archive', 0))
    extract = int(section.get('extract', 0))
    unique_path = int(section.get('unique_path', 1))

    if agent != 'manual':
        core.pause_torrent(agent, torrent_hash, torrent_id, name)

    # In case input is not directory, make sure to create one.
    # This way Processing is isolated.
    if not os.path.isdir(os.path.join(directory, name)):
        basename = os.path.basename(directory)
        basename = core.sanitizeName(name) \
            if name == basename else os.path.splitext(core.sanitizeName(name))[0]
        destination = os.path.join(core.OUTPUTDIRECTORY, category, basename)
    elif unique_path:
        destination = os.path.normpath(
            core.os.path.join(core.OUTPUTDIRECTORY, category, core.sanitizeName(name)))
    else:
        destination = os.path.normpath(
            core.os.path.join(core.OUTPUTDIRECTORY, category))
    try:
        destination = destination.encode(core.SYS_ENCODING)
    except UnicodeError:
        pass

    if destination in directory:
        destination = directory

    logger.info('Output directory set to: {0}'.format(destination))

    if core.SAFE_MODE and destination == core.TORRENT_DEFAULTDIR:
        logger.error('The output directory:[{0}] is the Download Directory. '
                     'Edit outputDirectory in autoProcessMedia.cfg. Exiting'.format
                     (directory))
        return [-1, '']

    logger.debug('Scanning files in directory: {0}'.format(directory))

    if section_name == 'HeadPhones':
        core.NOFLATTEN.extend(
            category)  # Make sure we preserve folder structure for HeadPhones.

    now = datetime.now()

    if extract == 1:
        input_files = core.listMediaFiles(directory, archives=False)
    else:
        input_files = core.listMediaFiles(directory)
    logger.debug('Found {0} files in {1}'.format(len(input_files), directory))
    for input_file in input_files:
        file_path = os.path.dirname(input_file)
        file_name, file_extension = os.path.splitext(os.path.basename(input_file))
        full_file_name = os.path.basename(input_file)

        target_file = core.os.path.join(destination, full_file_name)
        if category in core.NOFLATTEN:
            if not os.path.basename(file_path) in destination:
                target_file = core.os.path.join(
                    core.os.path.join(destination, os.path.basename(file_path)), full_file_name)
                logger.debug('Setting destination to {0} to preserve folder structure'.format
                             (os.path.dirname(target_file)))
        try:
            target_file = target_file.encode(core.SYS_ENCODING)
        except UnicodeError:
            pass
        if root == 1:
            if not found_file:
                logger.debug('Looking for {0} in: {1}'.format(name, input_file))
            if any([core.sanitizeName(name) in core.sanitizeName(input_file),
                    core.sanitizeName(file_name) in core.sanitizeName(name)]):
                found_file = True
                logger.debug('Found file {0} that matches Torrent Name {1}'.format
                             (full_file_name, name))
            else:
                continue

        if root == 2:
            mtime_lapse = now - datetime.fromtimestamp(os.path.getmtime(input_file))
            ctime_lapse = now - datetime.fromtimestamp(os.path.getctime(input_file))

            if not found_file:
                logger.debug('Looking for files with modified/created dates less than 5 minutes old.')
            if (mtime_lapse < timedelta(minutes=5)) or (ctime_lapse < timedelta(minutes=5)):
                found_file = True
                logger.debug('Found file {0} with date modified/created less than 5 minutes ago.'.format
                             (full_file_name))
            else:
                continue  # This file has not been recently moved or created, skip it

        if torrent_no_link == 0:
            try:
                core.copy_link(input_file, target_file, core.USELINK)
                core.rmReadOnly(target_file)
            except:
                logger.error('Failed to link: {0} to {1}'.format(input_file, target_file))

    name, destination = convert_to_ascii(name, destination)

    if extract == 1:
        logger.debug('Checking for archives to extract in directory: {0}'.format(directory))
        core.extractFiles(directory, destination, keep_archive)

    if category not in core.NOFLATTEN:
        # don't flatten hp in case multi cd albums, and we need to copy this back later.
        core.flatten(destination)

    # Now check if video files exist in destination:
    if section_name in ['SickBeard', 'NzbDrone', 'CouchPotato']:
        num_videos = len(core.listMediaFiles(destination, media=True, audio=False, meta=False, archives=False))
        if num_videos:
            logger.info('Found {0} media files in {1}'.format(num_videos, destination))
            status = 0
        elif extract != 1:
            logger.info('No media files found in {0}. Sending to {1} to process'.format(destination, section_name))
            status = 0
        else:
            logger.warning('No media files found in {0}'.format(destination))

    # Only these sections can handling failed downloads
    # so make sure everything else gets through without the check for failed
    if section_name not in ['CouchPotato', 'SickBeard', 'NzbDrone']:
        status = 0

    logger.info('Calling {0}:{1} to post-process:{2}'.format(section_name, usercat, name))

    if core.TORRENT_CHMOD_DIRECTORY:
        core.rchmod(destination, core.TORRENT_CHMOD_DIRECTORY)

    result = [0, '']
    if section_name == 'UserScript':
        result = external_script(destination, name, category, section)

    elif section_name == 'CouchPotato':
        result = core.autoProcessMovie().process(section_name, destination, name,
                                                 status, agent, torrent_hash, category)
    elif section_name in ['SickBeard', 'NzbDrone']:
        if torrent_hash:
            torrent_hash = torrent_hash.upper()
        result = core.autoProcessTV().processEpisode(section_name, destination, name,
                                                     status, agent, torrent_hash, category)
    elif section_name == 'HeadPhones':
        result = core.autoProcessMusic().process(section_name, destination, name,
                                                 status, agent, category)
    elif section_name == 'Mylar':
        result = core.autoProcessComics().processEpisode(section_name, destination, name,
                                                         status, agent, category)
    elif section_name == 'Gamez':
        result = core.autoProcessGames().process(section_name, destination, name,
                                                 status, agent, category)

    plex_update(category)

    if result[0] != 0:
        if not core.TORRENT_RESUME_ON_FAILURE:
            logger.error('A problem was reported in the autoProcess* script. '
                         'Torrent won\'t resume seeding (settings)')
        elif agent != 'manual':
            logger.error('A problem was reported in the autoProcess* script. '
                         'If torrent was paused we will resume seeding')
            core.resume_torrent(agent, torrent_hash, torrent_id, name)

    else:
        if agent != 'manual':
            # update download status in our DB
            core.update_downloadInfoStatus(name, 1)

            # remove torrent
            if core.USELINK == 'move-sym' and not core.DELETE_ORIGINAL == 1:
                logger.debug('Checking for sym-links to re-direct in: {0}'.format(directory))
                for path, directories, files in os.walk(directory):
                    for each_file in files:
                        logger.debug('Checking symlink: {0}'.format(os.path.join(path, each_file)))
                        replace_links(os.path.join(path, each_file))
            core.remove_torrent(agent, torrent_hash, torrent_id, name)

        if not section_name == 'UserScript':
            # for user script, we assume this is cleaned by the script or option USER_SCRIPT_CLEAN
            # cleanup our processing folders of any misc unwanted files and empty directories
            core.cleanDir(destination, section_name, category)

    return result


def main(args):
    # Initialize the config
    core.initialize()

    # agent for Torrents
    agent = core.TORRENT_CLIENTAGENT

    logger.info('#########################################################')
    logger.info('## ..::[{0:^41}]::.. ##'.format(os.path.basename(__file__)))
    logger.info('#########################################################')

    # debug command line options
    logger.debug('Options passed into TorrentToMedia: {0}'.format(args))

    # Post-Processing Result
    result = [0, '']

    try:
        directory, name, category, torrent_hash, torrent_id = core.parse_args(agent, args)
    except:
        logger.error('There was a problem loading variables')
        return -1

    if directory and name and torrent_hash and torrent_id:
        result = process_torrent(directory, name, category, torrent_hash, torrent_id, agent)
    else:
        # Perform Manual Post-Processing
        logger.warning('Invalid number of arguments received from client, Switching to manual run mode ...')

        for section, subsections in core.SECTIONS.items():
            for subsection in subsections:
                if not core.CFG[section][subsection].isenabled():
                    continue
                for directory in core.getDirs(section, subsection, link='hard'):
                    logger.info('Starting manual run for {0}:{1} - Folder:{2}'.format
                                (section, subsection, directory))

                    logger.info('Checking database for download info for {0} ...'.format
                                (os.path.basename(directory)))
                    core.DOWNLOADINFO = core.get_downloadInfo(os.path.basename(directory), 0)
                    if core.DOWNLOADINFO:
                        agent = text_type(core.DOWNLOADINFO[0].get('client_agent', 'manual'))
                        torrent_hash = text_type(core.DOWNLOADINFO[0].get('input_hash', ''))
                        torrent_id = text_type(core.DOWNLOADINFO[0].get('input_id', ''))
                        logger.info('Found download info for {0}, '
                                    'setting variables now ...'.format(os.path.basename(directory)))
                    else:
                        logger.info('Unable to locate download info for {0}, '
                                    'continuing to try and process this release ...'.format
                                    (os.path.basename(directory)))
                        agent = 'manual'
                        torrent_hash = ''
                        torrent_id = ''

                    if agent.lower() not in core.TORRENT_CLIENTS:
                        continue

                    try:
                        directory = directory.encode(core.SYS_ENCODING)
                    except UnicodeError:
                        pass
                    name = os.path.basename(directory)
                    try:
                        name = name.encode(core.SYS_ENCODING)
                    except UnicodeError:
                        pass

                    results = process_torrent(directory, name, subsection, torrent_hash or None, torrent_id or None,
                                              agent)
                    if results[0] != 0:
                        logger.error('A problem was reported when trying to perform a manual run for {0}:{1}.'.format
                                     (section, subsection))
                        result = results

    if result[0] == 0:
        logger.info('The {0} script completed successfully.'.format(args[0]))
    else:
        logger.error('A problem was reported in the {0} script.'.format(args[0]))
    del core.MYAPP
    return result[0]


if __name__ == '__main__':
    exit(main(sys.argv))
