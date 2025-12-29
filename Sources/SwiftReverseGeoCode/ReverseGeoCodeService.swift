import Foundation
import SQLite

public final class ReverseGeoCodeService {
	private let dbConnection: Connection

	/// Precompiled statement
	private let reverseStmt: Statement

	/// SQL is static so it never gets re-parsed
	private static let reverseSQL = """
		SELECT * FROM everything WHERE id IN (
			SELECT feature_id
			FROM coordinates
			WHERE latitude BETWEEN :lat - 1.5 AND :lat + 1.5
			  AND longitude BETWEEN :long - 1.5 AND :long + 1.5
			ORDER BY (
				(:lat - latitude) * (:lat - latitude) +
				(:long - longitude) * (:long - longitude) * :scale
			) ASC
			LIMIT 1
		)
		"""

	public init(database: String) throws {
		do {
			self.dbConnection = try Connection(database, readonly: true)
		} catch {
			throw error
		}

		do {
			self.reverseStmt = try dbConnection.prepare(Self.reverseSQL)
		} catch {
			throw error
		}
	}

	public func reverseGeoCode(latitude: Double, longitude: Double) throws -> LocationDescription {
		let scale = pow(cos(latitude * .pi / 180), 2.0)

		// Bind parameters in the order they appear in the SQL
		let rows = reverseStmt.bind(latitude, longitude, scale)

		for row in rows {
			guard
				let id = row[0] as? Int64,
				let name = row[1] as? String,
				let adminame = row[3] as? String,
				let countrycode = row[4] as? String,
				let countryname = row[5] as? String,
				let lat = row[6] as? Double,
				let long = row[7] as? Double
			else {
				throw ReverseError.errorUnwraping
			}

			return LocationDescription(
				id: id,
				name: name,
				adminName: adminame,
				countryCode: countrycode,
				countryName: countryname,
				latitude: lat,
				longitude: long
			)
		}

		throw ReverseError.novalue
	}
}
