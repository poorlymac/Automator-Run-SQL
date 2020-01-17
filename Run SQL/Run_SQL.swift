//
//  Run_SQL.swift
//  Run SQL
//
//  Created by Paul Schaap on 17/1/20.
//	Copyright © 2020 Paul Schaap. All rights reserved.
//
import Foundation
import Automator
import AppKit
import os.log

class Run_SQL: AMBundleAction {

	@IBOutlet weak var textInputField: NSTextField!
    @IBOutlet weak var clientVersion: NSTextField!
    @IBOutlet weak var rowCounter: NSTextField!
    @IBOutlet weak var rowLimit: NSTextField!
    @IBOutlet weak var outputFormat: NSPopUpButton!
    @IBOutlet weak var headersCheckbox: NSButton!
    @IBOutlet weak var delimiter: NSTextField!
    let supportedDatabases = ["mysql", "postgresql", "sqlite"]

    override func run(withInput input: Any?) throws -> Any {
        
    	// NOTE: In os_log() non-static values are marked <private>, so generated values must be explicitly marked public.
        let arrayOfValueable = input as! [String]
        os_log("The value of input is: %{public}@", arrayOfValueable[0])
        let SQL = arrayOfValueable[0]
        
        // ACTION PROPERTIES
        // Action properties are default properties of the action.
        
        // property with string value
        let actionName: String = name
        os_log("The value of action’s “name” property is: %{public}@", actionName)

        // property with boolean value
        let inputSetting: Bool  =  ignoresInput
        os_log("The value of action’s “ignoresInput” property is: %{public}@", inputSetting.description)
        
        // get the paramters dictionary
        guard let params = parameters else {
            throw NSError(domain:"Cannot unwrap parameters", code:-1, userInfo:nil)
        }

        // get the individual parameters from paramters dictionary
        guard let rowLimit: Int = params.object(forKey: "rowLimit") as? Int else {
            throw NSError(domain:"Cannot get rowLimit", code:-1, userInfo:nil)
        }
        guard let headers: Bool = params.object(forKey: "headers") as? Bool else {
            throw NSError(domain:"Cannot get headers setting", code:-1, userInfo:nil)
        }
        guard let connectionURL: String = params.object(forKey: "connectionURL") as? String else {
            throw NSError(domain:"Cannot get connectionURL", code:-1, userInfo:nil)
        }
        // NOTE: I am doing outputFormatas an IBOutlet, it may need to be a param like the above
        let format = outputFormat.titleOfSelectedItem

        // Connect to database
        let url = URL(string: connectionURL)
        let database = (url?.scheme ?? "").lowercased()
        if (!supportedDatabases.contains(database)) {
            throw NSError(domain:"Unsupported Database", code:-1, userInfo:nil)
        }
        
        var output:Any = ""
        if(database == "mysql") {
            let myConn = mysql_init(nil)
            if (myConn == nil) {
                throw NSError(domain:"Could not initialise database connection", code:-1, userInfo:nil)
            }
            if(mysql_real_connect(
                    myConn,
                    url?.host,
                    url?.user ?? "root",
                    url?.password ?? "root",
                    url?.path.components(separatedBy: "/")[1],
                    UInt32(url?.port ?? 3306),
                    nil,
                    0
                ) == nil) {
                throw NSError(domain:"Could not create database connection", code:0, userInfo:nil)
            }
            // Note this also closes the connection
            try output = query_mysql(myConn: myConn, SQL: SQL, format: format!, headers: headers, rowLimit: rowLimit)
        } else if (database == "postgresql") {
            let pqConn = PQconnectdb(connectionURL)
            if (PQstatus(pqConn) != CONNECTION_OK) {
                let errorMessage = String(validatingUTF8: PQerrorMessage(pqConn)) ?? ""
                throw NSError(domain:"Could not create database connection" + errorMessage, code:-1, userInfo:nil)
            }
            try output = query_postgresql(pqConn: pqConn, SQL: SQL, format: format!, headers: headers, rowLimit: rowLimit)
        } else if (database == "sqlite") {
            var slConn:OpaquePointer?
            let cRes = sqlite3_open_v2(url?.path ?? "", &slConn, SQLITE_OPEN_READWRITE, nil)
            if (cRes != SQLITE_OK) {
                let errorMessage = String(validatingUTF8: sqlite3_errmsg(slConn)) ?? ""
                throw NSError(domain:"Could not create database connection to \(url?.path ?? "") ERROR:\(errorMessage)", code:-1, userInfo:nil)
            }
            try output = query_sqlite(slConn: slConn, SQL: SQL, format: format!, headers: headers, rowLimit: rowLimit)
        }
        
        // LOCALIZED STRINGS
        // use getLocalizedStringForKey("KEY") to retrieve matched string in Localizable.strings file
        // let localString: String = getLocalizedStringForKey(key: "EXAMPLE_KEY")
        // os_log("Localized string: %{public}@", localString)

        return output
    }

    // Invoked when the action is first added to a workflow, allowing it to initialize its user interface.
	override func opened(){
        let myClientInfo = String(cString: mysql_get_client_info())
        // Compiler gets really slow doing it this way
//        let pqClientInfo = Float(Int(PQlibVersion()))
//        let pqVersion = "\(Int(round(pqClientInfo/10000))).\(Int(round(pqClientInfo/100) - round(pqClientInfo/10000) * 100)).\(Int(pqClientInfo - round(pqClientInfo/100)*100))"
        let pqClientInfo = String(Int(PQlibVersion()))
        let pqVersion = "\(Int(pqClientInfo.dropLast(4)) ?? 0).\(Int(pqClientInfo.suffix(4).prefix(2)) ?? 0).\(Int(pqClientInfo.suffix(2)) ?? 0)"
        let slClientInfo = String(validatingUTF8: sqlite3_libversion()) ?? ""
        clientVersion.stringValue = "MySQL: \(myClientInfo)\nPostgres: \(pqVersion)\nSQLite: \(slClientInfo)"
    }
    
    // Requests the action to update its user interface from its stored parameters, which have changed.
    override func parametersUpdated(){
    	print("parametersUpdated")
    }
    
    // Requests the action to update its stored set of parameters from the settings in the action’s user interface.
    override func updateParameters(){
    	print("updateParameters")
    }
    
    // Invoked by Automator when the receiving action is removed from a workflow, allowing it to perform cleanup operations.
    override func closed(){
    	print("closed")
    }
    
    // Invoked when the window of the Automator workflow to which the receiver belongs becomes the main window. This allows the action to synchronize its information with settings in another application.
    override func activated(){
        print("activated")
        
    }
    
	// Resets the action to its initial state.
    override func reset(){
        print("reset")
    }
        
    // Returns a localized version of the string designated by the specified key and residing in the specified table.
    // If tableName is nil or is an empty string, the method attempts to use the table in Localizable.strings.
    func getLocalizedStringForKey(key: String) -> String {
    	// use “bundle” property of AMBundleAction class to identify this action’s bundle
        let actionBundle: Bundle  =  bundle
        return NSLocalizedString(key, tableName: nil, bundle: actionBundle, value: "", comment: "")
    }
    
    // interface action code triggered by a button added to the action view
    @IBAction func actionButton(sender: AnyObject) {
        headersCheckbox.isEnabled = false
        delimiter.isEnabled = false
        if (outputFormat.titleOfSelectedItem == "CSV") {
            headersCheckbox.isEnabled = true
        }
        if (outputFormat.titleOfSelectedItem == "Text" || outputFormat.titleOfSelectedItem == "List") {
            headersCheckbox.isEnabled = true
            delimiter.isEnabled = true
        }
    }
    
    // basic informational dialog
	func basicAlertDialog(alertTitle: String, alertMessage: String) -> Bool {
		NSSound.beep()
		let alert = NSAlert()
		alert.messageText = alertTitle
		alert.informativeText = alertMessage
		alert.alertStyle = .informational
		alert.addButton(withTitle: "OK")
		alert.runModal()
		return true
	}
    
    // basic dialog with choice
	func basicTwoOptionDialogWithCancel(alertTitle: String, alertMessage: String, firstButtonTitle: String, secondButtonTitle: String) -> String {
		NSSound.beep()
		let alert = NSAlert()
		alert.messageText = alertTitle
		alert.informativeText = alertMessage
		alert.alertStyle = .informational
		alert.addButton(withTitle: firstButtonTitle)
		alert.addButton(withTitle: secondButtonTitle)
		alert.addButton(withTitle: "Cancel")
		let dialogResult =  alert.runModal()
		if dialogResult == NSApplication.ModalResponse.alertFirstButtonReturn {
			return firstButtonTitle
		} else if dialogResult == NSApplication.ModalResponse.alertSecondButtonReturn {
			return secondButtonTitle
		} else {
			return "Cancel"
		}
	}
    
    // basic yes or no alert dialog
	func basicConfirmationDialog(alertTitle: String, alertMessage: String) -> Bool {
		NSSound.beep()
		let alert: NSAlert = NSAlert()
		alert.messageText = alertTitle
		alert.informativeText = alertMessage
		alert.alertStyle = .informational // critical, warning, informational
		alert.addButton(withTitle:"OK")
		alert.addButton(withTitle:"Cancel")
		let dialogResult = alert.runModal()
		if dialogResult == NSApplication.ModalResponse.alertFirstButtonReturn {
			return true
		}
		return false
	}
    
    func query_mysql(myConn: UnsafeMutablePointer<MYSQL>?, SQL: String, format:String, headers:Bool, rowLimit:Int) throws -> Any {
        // Setup
        var str = ""
        var hdr = [Int: String]()
        var dat = [String: String]()
        var arr:[String] = []
        var dic = Array([])
        var del = delimiter.stringValue
        var ret = "\n"
        // Use CSV rules
        if(format == "CSV") {
            del = ","
            ret = "\r\n"
        }

        // Run query
        let queryResult = mysql_query(myConn, SQL)
        if (queryResult != 0) {
            os_log("Error Running ", SQL)
            mysql_close(myConn)
            throw NSError(domain:"Error (" + String(queryResult) + ") running: " + SQL, code:-1, userInfo:nil)
        }
        var x = 0;
        let result = mysql_store_result(myConn);
        if (result != nil) {
            let fieldCount:Int = Int(mysql_num_fields(result))

            // Setup headers
            var headertext = ""
            var colName = ""
            for field in 0...(fieldCount - 1) {
                let columnPtr = mysql_fetch_field_direct(result, UInt32(field))
                let f: MYSQL_FIELD = columnPtr!.pointee
                colName = String(validatingUTF8: f.name) ?? ""
                hdr[field] = colName
                if (format == "CSV") {
                    colName = "\"" + (colName).replacingOccurrences(of: "\"", with: "\"\"") + "\""
                }
                if (field == fieldCount - 1) {
                    if (format == "List") {
                        headertext += "\(colName)"
                    } else {
                        headertext += "\(colName)\(ret)"
                    }
                } else {
                    headertext += "\(colName)\(del)"
                }
            }
            if (headers && format != "Dictionary") {
                if (format == "List") {
                    arr.append(headertext)
                } else {
                    str += headertext
                }
            }
            
            // Get counts
            let rowCount = Int(mysql_num_rows(result))
            let numberFormatter = NumberFormatter()
            numberFormatter.numberStyle = .decimal
            let rowCountString = numberFormatter.string(from: NSNumber(value: rowCount))!
            rowCounter.stringValue = String(format: "Count: %@", arguments: [rowCountString])
            
            // Setup progress
            self.progressValue = 0
            let maxCount = rowCount
            let incrementFactor = Double(1) / Double(maxCount)
            
            var row = mysql_fetch_row(result)
            while row != nil {
                x += 1
                var col = ""
                var rws = ""
                for field in 0...(fieldCount - 1) {
                    if let tabPtr = row![field] {
                        col = String(validatingUTF8: tabPtr) ?? ""
                    } else {
                        col = ""
                    }
                    if (format == "Dictionary") {
                        dat[String(hdr[field]!)] = col
                    } else {
                        if (format == "CSV" && (col.contains("\"") || col.contains(",") || col.contains("\r") || col.contains("\n"))) {
                            col = "\"" + col.replacingOccurrences(of: "\"", with: "\"\"") + "\""
                        }
                        if (field == fieldCount - 1) {
                            if (format == "List") {
                                rws += "\(col)"
                            } else {
                                rws += "\(col)\(ret)"
                            }
                        } else {
                            rws += "\(col)\(del)"
                        }
                    }
                }
                if (format == "Dictionary") {
                    dic.append(dat)
                } else if (format == "List") {
                    arr.append(rws)
                } else {
                    str += rws
                }
                
                // Update progress
                self.progressValue = CGFloat(Double(x) * incrementFactor)
                
                // Break out if limit hit
                if ( rowLimit > 0 && x >= rowLimit) {
                    let rowCountString = numberFormatter.string(from: NSNumber(value: rowLimit))!
                    rowCounter.stringValue = String(format: rowCounter.stringValue + " (%@)", arguments: [rowCountString])
                    break
                }
                row = mysql_fetch_row(result)
            }
            mysql_free_result(result)
        }
        // Close the connection
        mysql_close(myConn)

        // Return in requested format
        if (format == "Dictionary") {
            return dic
        } else if (format == "List") {
            return arr
        } else {
            return str
        }
    }
    
    func query_postgresql(pqConn: OpaquePointer?, SQL: String, format:String, headers:Bool, rowLimit:Int) throws -> Any {
        // Setup
        var str = ""
        var hdr = [Int: String]()
        var dat = [String: String]()
        var arr:[String] = []
        var dic = Array([])
        var del = delimiter.stringValue
        var ret = "\n"
        // Use CSV rules
        if(format == "CSV") {
            del = ","
            ret = "\r\n"
        }
        let queryResult = PQexec(pqConn, SQL)
        if (PQresultStatus(queryResult) != PGRES_TUPLES_OK) {
            let errorMessage = String(validatingUTF8: PQerrorMessage(pqConn)) ?? ""
            PQclear(queryResult);
            PQfinish(pqConn);
            os_log("Error Running ", SQL, " ", errorMessage)
            throw NSError(domain:"Error (\(errorMessage)) running: " + SQL, code:-1, userInfo:nil)
        }
        var x = 0;
        let fieldCount = PQnfields(queryResult)
        // Setup headers
        var headertext = ""
        var colName = ""
        for field in 0...(fieldCount - 1) {
            colName = String(validatingUTF8:PQfname(queryResult, field)) ?? ""
            hdr[Int(field)] = colName
            if (format == "CSV") {
                colName = "\"" + (colName).replacingOccurrences(of: "\"", with: "\"\"") + "\""
            }
            if (field == fieldCount - 1) {
                if (format == "List") {
                    headertext += "\(colName)"
                } else {
                    headertext += "\(colName)\(ret)"
                }
            } else {
                headertext += "\(colName)\(del)"
            }
        }
        if (headers && format != "Dictionary") {
            if (format == "List") {
                arr.append(headertext)
            } else {
                str += headertext
            }
        }
        
        // Get counts
        let rowCount = Int(PQntuples(queryResult))
        let numberFormatter = NumberFormatter()
        numberFormatter.numberStyle = .decimal
        let rowCountString = numberFormatter.string(from: NSNumber(value: rowCount))!
        rowCounter.stringValue = String(format: "Count: %@", arguments: [rowCountString])
        
        // Setup progress
        self.progressValue = 0
        let maxCount = rowCount
        let incrementFactor = Double(1) / Double(maxCount)
        
        for row in 0...(rowCount - 1) {
            x += 1
            var col = ""
            var rws = ""
            for field in 0...(fieldCount - 1) {
                if let tabPtr = PQgetvalue(queryResult, Int32(row), field) {
                    col = String(validatingUTF8: tabPtr) ?? ""
                } else {
                    col = ""
                }
                if (format == "Dictionary") {
                    dat[String(hdr[Int(field)]!)] = col
                } else {
                    if (format == "CSV" && (col.contains("\"") || col.contains(",") || col.contains("\r") || col.contains("\n"))) {
                        col = "\"" + col.replacingOccurrences(of: "\"", with: "\"\"") + "\""
                    }
                    if (field == fieldCount - 1) {
                        if (format == "List") {
                            rws += "\(col)"
                        } else {
                            rws += "\(col)\(ret)"
                        }
                    } else {
                        rws += "\(col)\(del)"
                    }
                }
            }
            if (format == "Dictionary") {
                dic.append(dat)
            } else if (format == "List") {
                arr.append(rws)
            } else {
                str += rws
            }
            
            // Update progress
            self.progressValue = CGFloat(Double(x) * incrementFactor)
            
            // Break out if limit hit
            if ( rowLimit > 0 && x >= rowLimit) {
                let rowCountString = numberFormatter.string(from: NSNumber(value: rowLimit))!
                rowCounter.stringValue = String(format: rowCounter.stringValue + " (%@)", arguments: [rowCountString])
                break
            }
        }
        
        PQclear(queryResult);
        PQfinish(pqConn);
        
        // Return in requested format
        if (format == "Dictionary") {
            return dic
        } else if (format == "List") {
            return arr
        } else {
            return str
        }
    }
    
    func query_sqlite(slConn: OpaquePointer?, SQL: String, format:String, headers:Bool, rowLimit:Int) throws -> Any {
        // Setup
        var str = ""
        var hdr = [Int: String]()
        var dat = [String: String]()
        var arr:[String] = []
        var dic = Array([])
        var del = delimiter.stringValue
        var ret = "\n"
        // Use CSV rules
        if(format == "CSV") {
            del = ","
            ret = "\r\n"
        }
        
        var queryResult:OpaquePointer?
        var unused:UnsafePointer<Int8>?
        let queryPrepare = sqlite3_prepare_v2(slConn, SQL, -1, &queryResult, &unused)
        if (queryPrepare != SQLITE_OK ) {
            let errorMessage = String(validatingUTF8: sqlite3_errmsg(slConn)) ?? ""
            sqlite3_free(&queryResult)
            sqlite3_close(slConn)
            os_log("Error Running ", SQL, " ", errorMessage)
            throw NSError(domain:"Error (\(errorMessage)) running: " + SQL, code:-1, userInfo:nil)
        }
        var x = 0;
        let fieldCount = sqlite3_column_count(queryResult)
        // Setup headers
        var headertext = ""
        var colName = ""
        for field in 0...(fieldCount - 1) {
            colName = String(cString:sqlite3_column_name(queryResult, field))
            hdr[Int(field)] = colName
            if (format == "CSV") {
                colName = "\"" + (colName).replacingOccurrences(of: "\"", with: "\"\"") + "\""
            }
            if (field == fieldCount - 1) {
                if (format == "List") {
                    headertext += "\(colName)"
                } else {
                    headertext += "\(colName)\(ret)"
                }
            } else {
                headertext += "\(colName)\(del)"
            }
        }
        if (headers && format != "Dictionary") {
            if (format == "List") {
                arr.append(headertext)
            } else {
                str += headertext
            }
        }

        var rowCount = 0
        var row = sqlite3_step(queryResult)
        while (row == SQLITE_ROW) {
            rowCount+=1
            if(rowLimit == 0 || rowCount <= rowLimit) {
                x+=1
                var col = ""
                var rws = ""
                for field in 0...(fieldCount - 1) {
                    if (sqlite3_column_type(queryResult, field) == SQLITE_NULL) {
                        col = ""
                    } else {
                        col = String(cString: sqlite3_column_text(queryResult, field))
                    }
                    if (format == "Dictionary") {
                        dat[String(hdr[Int(field)]!)] = col
                    } else {
                        if (format == "CSV" && (col.contains("\"") || col.contains(",") || col.contains("\r") || col.contains("\n"))) {
                            col = "\"" + col.replacingOccurrences(of: "\"", with: "\"\"") + "\""
                        }
                        if (field == fieldCount - 1) {
                            if (format == "List") {
                                rws += "\(col)"
                            } else {
                                rws += "\(col)\(ret)"
                            }
                        } else {
                            rws += "\(col)\(del)"
                        }
                    }
                }
                if (format == "Dictionary") {
                    dic.append(dat)
                } else if (format == "List") {
                    arr.append(rws)
                } else {
                    str += rws
                }
            }
            row = sqlite3_step(queryResult)
        }
        sqlite3_finalize(queryResult)
        sqlite3_close(slConn)

        // Get counts
        let numberFormatter = NumberFormatter()
        numberFormatter.numberStyle = .decimal
        let rowCountString = numberFormatter.string(from: NSNumber(value: rowCount))!
        let retCountString = numberFormatter.string(from: NSNumber(value: x))!
        rowCounter.stringValue = String(format: "Count: %@ (%@)", arguments: [rowCountString, retCountString])

        // Return in requested format
        if (format == "Dictionary") {
            return dic
        } else if (format == "List") {
            return arr
        } else {
            return str
        }
    }
}
