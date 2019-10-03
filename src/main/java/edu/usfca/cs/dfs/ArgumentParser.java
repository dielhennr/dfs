package edu.usfca.cs.dfs;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

public class ArgumentParser {

	private final Map<String, String> map;

	/**
	 * Initializes this argument map.
	 */
	public ArgumentParser() {
		this.map = new TreeMap<String, String>();

	}

	/**
	 * Initializes this argument map and then parsers the arguments into flag/value
	 * pairs where possible. Some flags may not have associated values. If a flag is
	 * repeated, its value is overwritten.
	 *
	 * @param args
	 */
	public ArgumentParser(String[] args) {
		this();
		parse(args);
	}

	/**
	 * Parses the arguments into flag/value pairs where possible. Some flags may not
	 * have associated values. If a flag is repeated, its value is overwritten.
	 *
	 * @param args the command line arguments to parse
	 */
	public void parse(String[] args) {

		int length = args.length;
		if (length == 0)
			return;

		int i = 0;
		for (i = 0; i < length - 1; i++) {
			if (args[i].startsWith("-") && !args[i + 1].startsWith("-")) {
				if (!map.containsKey(args[i]))
					map.put(args[i], args[i + 1]);
			} else if (args[i].startsWith("-") && args[i].startsWith("-")) {
				if (!map.containsKey(args[i]))
					map.put(args[i], null);
			}
		}

		if (args[i].startsWith("-")) {
			if (!map.containsKey(args[i]))
				map.put(args[i], null);
		}
	}

	/**
	 * Determines whether the argument is a flag. Flags start with a dash "-"
	 * character, followed by at least one other non-whitespace character.
	 *
	 * @param arg the argument to test if its a flag
	 * @return {@code true} if the argument is a flag
	 *
	 * @see String#startsWith(String)
	 * @see String#trim()
	 * @see String#isEmpty()
	 * @see String#length()
	 */
	public static boolean isFlag(String arg) {

		if (arg == null) {
			return false;
		}

		arg = arg.trim();

		return arg.length() > 1 && arg.startsWith("-");

	}

	/**
	 * Determines whether the argument is a value. Values do not start with a dash
	 * "-" character, and must consist of at least one non-whitespace character.
	 *
	 * @param arg the argument to test if its a value
	 * @return {@code true} if the argument is a value
	 *
	 * @see String#startsWith(String)
	 * @see String#trim()
	 * @see String#isEmpty()
	 * @see String#length()
	 */
	public static boolean isValue(String arg) {

		if (arg == null) {
			return false;
		}

		arg = arg.trim();

		return (!arg.startsWith("-") && arg.length() > 1);

	}

	/**
	 * Returns the number of unique flags.
	 *
	 * @return number of unique flags
	 */
	public int numFlags() {

		return map.size();

	}

	public int parseInt() {

		return 0;
	}

	/**
	 * Determines whether the specified flag exists.
	 *
	 * @param flag the flag to search for
	 * @return {@code true} if the flag exists
	 */
	public boolean hasFlag(String flag) {

		return map.containsKey(flag);

	}

	/**
	 * Determines whether the specified flag is mapped to a non-null value.
	 *
	 * @param flag the flag to search for
	 * @return {@code true} if the flag is mapped to a non-null value
	 */
	public boolean hasValue(String flag) {

		return map.get(flag) != null;

	}

	/**
	 * Returns the value to which the specified flag is mapped as a {@link String},
	 * or null if there is no mapping for the flag.
	 *
	 * @param flag the flag whose associated value is to be returned
	 * @return the value to which the specified flag is mapped, or {@code null} if
	 *         there is no mapping for the flag
	 */
	public String getString(String flag) {

		return (map.get(flag));

	}

	/**
	 * Returns the value to which the specified flag is mapped as a {@link String},
	 * or the default value if there is no mapping for the flag.
	 *
	 * @param flag         the flag whose associated value is to be returned
	 * @param defaultValue the default value to return if there is no mapping for
	 *                     the flag
	 * @return the value to which the specified flag is mapped, or the default value
	 *         if there is no mapping for the flag
	 */
	public String getString(String flag, String defaultValue) {

		return (map.get(flag) == null ? defaultValue : map.get(flag));

	}

	/**
	 * Returns the value to which the specified flag is mapped as a {@link Path}, or
	 * {@code null} if unable to retrieve this mapping for any reason (including
	 * being unable to convert the value to a {@link Path} or no value existing for
	 * this flag).
	 *
	 * This method should not throw any exceptions!
	 *
	 * @param flag the flag whose associated value is to be returned
	 * @return the value to which the specified flag is mapped, or {@code null} if
	 *         unable to retrieve this mapping for any reason
	 *
	 * @see Paths#get(String, String...)
	 */
	public Path getPath(String flag) {

		try {

			return Paths.get(map.get(flag));

		} catch (InvalidPathException | NullPointerException np) {
			System.out.println("error: " + np.getMessage());
			return null;
		}

	}

	/**
	 * Returns the value to which the specified flag is mapped as a {@link Path}, or
	 * the default value if unable to retrieve this mapping for any reason
	 * (including being unable to convert the value to a {@link Path} or no value
	 * existing for this flag).
	 *
	 * This method should not throw any exceptions!
	 *
	 * @param flag         the flag whose associated value is to be returned
	 * @param defaultValue the default value to return if there is no mapping for
	 *                     the flag
	 * @return the value to which the specified flag is mapped as a {@link Path}, or
	 *         the default value if there is no mapping for the flag
	 */
	public Path getPath(String flag, Path defaultValue) {

		try {

			return Paths.get(map.get(flag));

		} catch (NullPointerException e) {
			return defaultValue;
		}
	}

	/**
	 * Returns the flag value if it contains a value
	 * 
	 * @param flag
	 * @return
	 */
	public String getValue(String flag) {
		if (map.containsKey(flag) && map.get(flag) != null && hasValue(flag) == true) {
			return map.get(flag);
		}
		return null;
	}

	@Override
	public String toString() {
		return this.map.toString();
	}
}