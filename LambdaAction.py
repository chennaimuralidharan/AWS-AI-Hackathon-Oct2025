import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import logging
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to retrieve course data from MyCourses table for Bedrock CourseAgent
    Table structure: CourseID, Course Name, Duration (hours), State
    """
    logger.info(f"Received event: {json.dumps(event, default=str)}")
    
    try:
        # Initialize DynamoDB resource - CORRECTED TABLE NAME
        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('MyCourses')  # Changed from 'myCourse' to 'MyCourses'
            logger.info("DynamoDB connection established for MyCourses table")
        except Exception as db_error:
            logger.error(f"Failed to connect to DynamoDB: {str(db_error)}")
            return create_error_response("Database connection failed", str(db_error))
        
        # Extract information from Bedrock Agent event
        action_group = event.get('actionGroup', 'CourseActionGroup')
        api_path = event.get('apiPath', '')
        http_method = event.get('httpMethod', 'GET')
        parameters = event.get('parameters', [])
        
        # Convert parameters list to dictionary
        params = {}
        for param in parameters:
            params[param['name']] = param['value']
        
        logger.info(f"Processing - API Path: {api_path}, Method: {http_method}, Params: {params}")
        
        # Route to appropriate function
        if api_path == '/getAllCourses':
            result = get_all_courses(table)
        elif api_path == '/getCoursesByState':
            result = get_courses_by_state(table, params)
        elif api_path == '/getCourseDetails':
            result = get_course_details(table, params)
        elif api_path == '/getCompletedCourses':
            result = get_courses_by_state(table, {'state': 'Completed'})
        elif api_path == '/getInProgressCourses':
            result = get_courses_by_state(table, {'state': 'In Progress'})
        elif api_path == '/getNotStartedCourses':
            result = get_courses_by_state(table, {'state': 'Not Started'})
        else:
            result = {
                'success': False,
                'error': f'Unknown API path: {api_path}',
                'availablePaths': ['/getAllCourses', '/getCoursesByState', '/getCourseDetails', '/getCompletedCourses', '/getInProgressCourses', '/getNotStartedCourses']
            }
        
        return create_bedrock_response(action_group, api_path, http_method, result)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        error_result = {
            'success': False,
            'error': f'Internal error: {str(e)}',
            'type': type(e).__name__
        }
        return create_bedrock_response(
            event.get('actionGroup', 'CourseActionGroup'), 
            event.get('apiPath', ''), 
            event.get('httpMethod', 'GET'), 
            error_result
        )

def get_all_courses(table):
    """Get all courses from MyCourses table"""
    try:
        logger.info("Scanning MyCourses table for all courses")
        
        # Test table access first
        try:
            table_info = table.table_status
            logger.info(f"Table status: {table_info}")
        except Exception as table_error:
            logger.error(f"Cannot access table: {str(table_error)}")
            return {
                'success': False,
                'error': f'Cannot access MyCourses table: {str(table_error)}',
                'suggestion': 'Check if table exists and IAM permissions are correct'
            }
        
        # Scan the table
        response = table.scan()
        courses = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            courses.extend(response.get('Items', []))
        
        # Convert Decimal types and normalize field names
        courses = convert_decimals(courses)
        normalized_courses = []
        
        for course in courses:
            normalized_course = {
                'CourseID': course.get('CourseID', ''),
                'CourseName': course.get('Course Name', ''),  # Note the space in field name
                'Duration': course.get('Duration (hours)', ''),  # Note the parentheses
                'State': course.get('State', '')
            }
            normalized_courses.append(normalized_course)
        
        # Sort by CourseID
        normalized_courses.sort(key=lambda x: x.get('CourseID', ''))
        
        logger.info(f"Successfully retrieved {len(normalized_courses)} courses")
        
        return {
            'success': True,
            'data': {
                'courses': normalized_courses,
                'totalCourses': len(normalized_courses),
                'message': f'Retrieved {len(normalized_courses)} courses successfully from MyCourses table'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in get_all_courses: {str(e)}")
        return {
            'success': False,
            'error': f'Failed to retrieve courses: {str(e)}',
            'function': 'get_all_courses'
        }

def get_courses_by_state(table, params):
    """Get courses filtered by state"""
    try:
        state = params.get('state')
        if not state:
            return {
                'success': False,
                'error': 'state parameter is required',
                'validStates': ['Completed', 'In Progress', 'Not Started']
            }
        
        logger.info(f"Getting courses with state: {state}")
        
        # Scan with filter - note the exact field name "State"
        response = table.scan(
            FilterExpression=Attr('State').eq(state)
        )
        
        courses = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=Attr('State').eq(state),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            courses.extend(response.get('Items', []))
        
        # Convert and normalize
        courses = convert_decimals(courses)
        normalized_courses = []
        
        for course in courses:
            normalized_course = {
                'CourseID': course.get('CourseID', ''),
                'CourseName': course.get('Course Name', ''),
                'Duration': course.get('Duration (hours)', ''),
                'State': course.get('State', '')
            }
            normalized_courses.append(normalized_course)
        
        normalized_courses.sort(key=lambda x: x.get('CourseID', ''))
        
        logger.info(f"Found {len(normalized_courses)} courses with state: {state}")
        
        return {
            'success': True,
            'data': {
                'courses': normalized_courses,
                'totalCourses': len(normalized_courses),
                'state': state,
                'message': f'Found {len(normalized_courses)} courses with state: {state}'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in get_courses_by_state: {str(e)}")
        return {
            'success': False,
            'error': f'Failed to filter courses by state: {str(e)}',
            'function': 'get_courses_by_state'
        }

def get_course_details(table, params):
    """Get details for a specific course"""
    try:
        course_id = params.get('courseId')
        if not course_id:
            return {
                'success': False,
                'error': 'courseId parameter is required'
            }
        
        logger.info(f"Getting details for course: {course_id}")
        
        response = table.get_item(
            Key={'CourseID': course_id}
        )
        
        if 'Item' not in response:
            return {
                'success': True,
                'data': {
                    'courseId': course_id,
                    'found': False,
                    'message': f'Course with ID {course_id} not found'
                }
            }
        
        course = convert_decimals(response['Item'])
        
        # Normalize the course data
        normalized_course = {
            'CourseID': course.get('CourseID', ''),
            'CourseName': course.get('Course Name', ''),
            'Duration': course.get('Duration (hours)', ''),
            'State': course.get('State', '')
        }
        
        return {
            'success': True,
            'data': {
                'courseId': course_id,
                'found': True,
                'course': normalized_course,
                'message': 'Course details retrieved successfully'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in get_course_details: {str(e)}")
        return {
            'success': False,
            'error': f'Failed to get course details: {str(e)}',
            'function': 'get_course_details'
        }

def convert_decimals(obj):
    """Convert DynamoDB Decimal types to float"""
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def create_bedrock_response(action_group, api_path, http_method, result):
    """Create properly formatted response for Bedrock Agent"""
    status_code = 200 if result.get('success', False) else 400
    
    response = {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': action_group,
            'apiPath': api_path,
            'httpMethod': http_method,
            'httpStatusCode': status_code,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(result, default=str)
                }
            }
        }
    }
    
    logger.info(f"Returning response with status {status_code}")
    return response

def create_error_response(error_message, details=None):
    """Create error response"""
    error_result = {
        'success': False,
        'error': error_message,
        'details': details
    }
    
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': 'CourseActionGroup',
            'apiPath': '/error',
            'httpMethod': 'GET',
            'httpStatusCode': 500,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(error_result)
                }
            }
        }
    }
