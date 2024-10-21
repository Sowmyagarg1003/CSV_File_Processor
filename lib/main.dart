import 'dart:async'; // Import for Timer
import 'dart:convert'; // For JSON decoding
import 'dart:io'; // For File handling

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;

void main() {
  WidgetsFlutterBinding.ensureInitialized(); // Ensure Flutter engine is initialized
  runApp(MyApp());
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'File Processor',
      home: FileProcessor(),
    );
  }
}

class FileProcessor extends StatefulWidget {
  @override
  _FileProcessorState createState() => _FileProcessorState();
}

class _FileProcessorState extends State<FileProcessor> {
  String _status = 'Select a file to process';
  Map<String, List<String>> folderContents = {
    'data': [],
    'done': [],
    'error': []
  };
  Timer? _timer;

  @override
  void initState() {
    super.initState();
    _startPolling(); // Start polling when the widget is initialized
  }

  @override
  void dispose() {
    _timer?.cancel();
    super.dispose();
  }

  void _startPolling() {
    _timer = Timer.periodic(Duration(seconds: 5), (timer) {
      _fetchFolderContents();
    });
  }

  Future<void> _fetchFolderContents() async {
    try {
      final response =
          await http.get(Uri.parse('http://192.168.1.9:8000/folder-contents'));
      if (response.statusCode == 200) {
        final data = json.decode(response.body);
        setState(() {
          // Convert List<dynamic> to List<String>
          folderContents['data'] = List<String>.from(data['data']);
          folderContents['done'] = List<String>.from(data['done']);
          folderContents['error'] = List<String>.from(data['error']);
        });
      } else {
        setState(() {
          _status = "Failed to fetch folder contents.";
        });
      }
    } catch (e) {
      setState(() {
        _status = "Exception occurred: $e";
      });
    }
  }

  Future<void> _uploadFile() async {
    FilePickerResult? result = await FilePicker.platform.pickFiles(
      type: FileType.any, // Allow all file types
    );

    if (result != null) {
      String? filePath = result.files.single.path;
      if (filePath != null) {
        File file = File(filePath);
        String fileName = file.path.split('/').last; // Get just the filename

        setState(() {
          _status = "File picked: ${file.path}";
          folderContents['data']!.add(fileName); // Add to 'data' folder initially
        });

        var request = http.MultipartRequest(
            'POST',
            Uri.parse(
                'http://192.168.1.9:8000/process')); 
        request.headers['X-Requested-With'] = 'XMLHttpRequest';
        request.files.add(await http.MultipartFile.fromPath('file', file.path));

        try {
          var response = await request.send().timeout(Duration(seconds: 30));
          var responseBody = await http.Response.fromStream(response);

          if (response.statusCode == 202) {
            setState(() {
              _status = "Processing started for file: $fileName";
            });
          } else if (response.statusCode == 400) {
            // Error due to invalid file type
            Map<String, dynamic> data = json.decode(responseBody.body);
            setState(() {
              _status = data["message"];
              folderContents['data']!.remove(fileName);
              folderContents['error']!.add(fileName);
            });
          } else {
            // Handle any other unexpected status codes
            setState(() {
              _status = "Unexpected response: ${response.statusCode}";
              folderContents['data']!.remove(fileName);
              folderContents['error']!.add(fileName);
            });
          }
        } catch (e) {
          // Handle exceptions during the request
          setState(() {
            _status = "Exception occurred during file processing: $e";
            folderContents['data']!.remove(fileName);
            folderContents['error']!.add(fileName);
          });
        }
      } else {
        setState(() {
          _status = "Could not retrieve the file path.";
        });
      }
    } else {
      setState(() {
        _status = "No file selected.";
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Center(child: Text('File Processor'))),
      body: Column(
        children: <Widget>[
          ElevatedButton(
            onPressed: _uploadFile,
            child: Center(child: Text('Select File')),
          ),
          const SizedBox(height: 20),
          Text(_status), // Display the status message
          const SizedBox(height: 20),
          Expanded(
            child: ListView(
              children: <Widget>[
                _buildFolderSection('Data Files', folderContents['data']!),
                _buildFolderSection('Done Files', folderContents['done']!),
                _buildFolderSection('Error Files', folderContents['error']!),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildFolderSection(String title, List<String> files) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          title,
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        ...files.map((file) => Text(file)).toList(),
        const SizedBox(height: 10),
      ],
    );
  }
}
