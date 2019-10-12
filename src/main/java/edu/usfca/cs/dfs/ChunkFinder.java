package edu.usfca.cs.dfs;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A utility class for finding all text files in a directory using lambda
 * functions and streams.
 *
 * @author CS 212 Software Development // Ryan Dielhenn and Sophie Engle
 * @author University of San Francisco
 * @version Fall 2019
 */
public class ChunkFinder {

	/**
	 * A lambda function that returns true if the path is a valid path and fits the
	 * standards of our system. IE file_name.txt@02@123#asdfe22r3rasdff... is valid
	 * while file_name.txt-1@3=@@##1233 (case-sensitive).
	 *
	 * @see Files#isRegularFile(Path, java.nio.file.LinkOption...)
	 */
	public static final Predicate<Path> TEXT_EXT = (file) -> Files.isRegularFile(file);

	/**
	 * Returns a stream of text files, following any symbolic links encountered.
	 *
	 * @param start the initial path to start with
	 * @return a stream of text files
	 *
	 * @throws IOException
	 *
	 * @see #TEXT_EXT
	 * @see FileVisitOption#FOLLOW_LINKS
	 *
	 *      We will set the second parameter to one so that we only get the files in
	 *      the base directory and not children directory if some end up there
	 * @see Files#find(Path, int, java.util.function.BiPredicate,
	 *      FileVisitOption...)
	 *
	 */
	public static Stream<Path> find(Path start) throws IOException {
		return Files.find(start, 1, (path, attribute) -> TEXT_EXT.test(path), FileVisitOption.FOLLOW_LINKS);
	}

	/**
	 * Returns a list of text files.
	 *
	 * @param start the initial path to search
	 * @return list of text files
	 * @throws IOException
	 *
	 * @see #find(Path)
	 */
	public static List<Path> list(Path start) throws IOException {
		ArrayList<Path> pathList = new ArrayList<Path>();
		ChunkFinder.find(start).forEach(e -> pathList.add(e));
		return pathList;
	}
}
