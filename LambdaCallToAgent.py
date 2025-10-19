import json
import boto3
import logging
import os
import time
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables with defaults


AWS_REGION = os.environ.get('AWS_REGION', 'ap-south-1')
MAX_PROMPT_LENGTH = int(os.environ.get('MAX_PROMPT_LENGTH', '4000'))
SESSION_TIMEOUT = int(os.environ.get('SESSION_TIMEOUT', '3600'))  # 1 hour in seconds

# Configure Bedrock Agent Runtime client with connection pooling and retry logic
config = Config(
    region_name=AWS_REGION,
    retries={'max_attempts': 3, 'mode': 'adaptive'},
    max_pool_connections=50
)

# Initialize Bedrock Agent Runtime client outside handler for connection reuse
bedrock_agent_runtime = boto3.client('bedrock-agent-runtime', config=config)
    
def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    start_time = time.time()
    
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")
        
        # Set up CORS headers
        headers = {
            'Content-Type': 'application/json',
           # 'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        }
        
        # Handle preflight OPTIONS request
        if event.get('httpMethod') == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'message': 'CORS preflight successful'})
            }
        
        # Parse request body
        body = parse_request_body(event)
        if not body:
            return create_error_response("Invalid request format", headers, 400)
        
        # Validate request
        is_valid, error_message = validate_request(body)
        if not is_valid:
            return create_error_response(error_message, headers, 400)
        
        prompt = body['prompt'].strip()
        session_id = body.get('sessionId', f"session_{int(time.time())}")
        
        logger.info(f"Processing request for session: {session_id} using agent: {BEDROCK_AGENT_NAME}")
        

        # Check if query is technical/career related
        if not is_technical_query(prompt):
            logger.info(f"Off-topic query detected for session {session_id}")
            off_topic_response = generate_off_topic_response(prompt)
            return create_success_response(off_topic_response, session_id, headers)
        
        # Generate AI response using Bedrock Agent
        ai_response = generate_ai_response_with_agent(prompt, session_id)
        
        # Log performance metrics
        execution_time = time.time() - start_time
        logger.info(f"Function executed successfully in {execution_time:.2f} seconds for session {session_id}")
        
        return create_success_response(ai_response, session_id, headers)
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Function failed after {execution_time:.2f} seconds: {str(e)}")
        return create_error_response(
            "An unexpected error occurred. Please try again later.", 
            headers if 'headers' in locals() else {}, 
            500
        )

def validate_request(body):
    """
    Enhanced input validation
    """
    if not body:
        return False, "Request body is required"
    
    if 'prompt' not in body:
        return False, "Prompt is required"
    
    prompt = body['prompt'].strip()
    if not prompt:
        return False, "Prompt cannot be empty"
    
    if len(prompt) > MAX_PROMPT_LENGTH:
        return False, f"Prompt too long (max {MAX_PROMPT_LENGTH} characters)"
    
    # Check for potentially harmful content
    harmful_patterns = ['<script', 'javascript:', 'data:', 'vbscript:']
    prompt_lower = prompt.lower()
    for pattern in harmful_patterns:
        if pattern in prompt_lower:
            return False, "Invalid content detected in prompt"
    
    return True, None

def parse_request_body(event):
    """
    Parse request body from different event sources
    """
    try:
        # API Gateway format
        if 'body' in event:
            if event['body']:
                body = json.loads(event['body'])
            else:
                return None
        # Lambda Function URL format
        elif 'requestContext' in event and 'http' in event['requestContext']:
            if event.get('body'):
                body = json.loads(event['body'])
            else:
                return None
        # Direct invocation
        elif 'prompt' in event:
            body = event
        else:
            return None
        
        return body
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error parsing request body: {str(e)}")
        return None

def is_technical_query(prompt):
    """
    Determine if the query is related to technical/career topics
    """
    prompt_lower = prompt.lower()
    
    # Non-technical keywords that should be rejected
    non_technical_keywords = [
        'cook', 'recipe', 'food', 'meal', 'kitchen', 'bake', 'ingredient',
        'dating', 'relationship', 'love', 'romance', 'marriage',
        'medical', 'health', 'doctor', 'medicine', 'symptom', 'disease',
        'legal', 'lawyer', 'law', 'court', 'lawsuit',
        'investment', 'stock', 'crypto', 'bitcoin', 'trading',
        'weather', 'sports', 'game', 'movie', 'music', 'entertainment',
        'travel', 'vacation', 'hotel', 'flight',
        'shopping', 'fashion', 'clothes', 'style'
    ]
    
    # Technical keywords that should be accepted
    technical_keywords = [
        'programming', 'code', 'software', 'development', 'developer',
        'python', 'java', 'javascript', 'react', 'node', 'aws', 'cloud',
        'ai', 'artificial intelligence', 'machine learning', 'ml', 'data science',
        'algorithm', 'database', 'sql', 'api', 'framework', 'library',
        'career', 'job', 'interview', 'resume', 'certification', 'course',
        'skill', 'learn', 'study', 'education', 'training', 'bootcamp',
        'technology', 'tech', 'computer', 'engineering', 'devops',
        'cybersecurity', 'security', 'network', 'system', 'architecture',
        'agile', 'scrum', 'project management', 'leadership', 'management'
    ]
    
    # Check for non-technical keywords first
    for keyword in non_technical_keywords:
        if keyword in prompt_lower:
            return False
    
    # Check for technical keywords
    for keyword in technical_keywords:
        if keyword in prompt_lower:       
            return True
    
    # If no specific keywords found, assume it might be technical
    # (better to be inclusive than exclusive for edge cases)
    return True

def generate_off_topic_response(prompt):
    """
    Generate a polite redirect response for off-topic queries
    """
    prompt_lower = prompt.lower()
    topic = "that topic"
    
    if 'cook' in prompt_lower or 'recipe' in prompt_lower or 'food' in prompt_lower:
        topic = "cooking"
    elif 'dating' in prompt_lower or 'relationship' in prompt_lower:
        topic = "relationships"
    elif 'health' in prompt_lower or 'medical' in prompt_lower:
        topic = "health advice"
    elif 'legal' in prompt_lower or 'law' in prompt_lower:
        topic = "legal advice"
    elif 'investment' in prompt_lower or 'stock' in prompt_lower:
        topic = "investment advice"
    elif 'weather' in prompt_lower:
        topic = "weather"
    elif 'sports' in prompt_lower or 'game' in prompt_lower:
        topic = "sports or games"
    
    return f"""I'm an AI Upskill Coach focused on technical career development. I can't help with {topic}, but I'd be happy to assist you with:

🚀 **AI & Machine Learning Careers**
• Career paths in AI/ML, data science, and analytics
• Required skills and certifications
• Industry trends and opportunities

💻 **Programming & Development**
• Programming languages (Python, JavaScript, Java, etc.)
• Frameworks and tools
• Best practices and coding skills

📚 **Learning & Certifications**
• Technical courses and bootcamps
• AWS, Google Cloud, Microsoft certifications
• Online learning platforms and resources

📝 **Professional Development**
• Resume optimization for tech roles
• Technical interview preparation
• Career progression strategies

What technical topic would you like to explore today?"""

def generate_ai_response_with_agent(prompt, session_id):
    """
    Generate AI response using Amazon Bedrock Agent 'myagent-invoke-llm' with enhanced error handling
    """
    try:
        logger.info(f"Invoking Bedrock Agent '{BEDROCK_AGENT_NAME}' (ID: {BEDROCK_AGENT_ID}) with alias {BEDROCK_AGENT_ALIAS_ID} for session {session_id}")
      
        # ~~~SMD START
        mycourse_keywords = [
            "my course", "my all course", "my completed course", 
            "my in progress course","my ongoing course",
            "all my course","my course list",
        ]   

        myprompt = prompt.lower()

        # ~~~SMD END

        # Invoke the Bedrock Agent
        if any(keyword in myprompt for keyword in mycourse_keywords):
            response = bedrock_agent_runtime.invoke_agent(
                agentId=BEDROCK_COURSEAGENT_ID,
                agentAliasId=BEDROCK_COURSEAGENT_ALIAS_ID,
                sessionId=session_id,
                inputText=prompt,
                enableTrace=False,  # Set to True for debugging if needed
                endSession=False    # Keep session alive for follow-up questions
            )
        else:
            response = bedrock_agent_runtime.invoke_agent(
                agentId=BEDROCK_AGENT_ID,
                agentAliasId=BEDROCK_AGENT_ALIAS_ID,
                sessionId=session_id,
                inputText=prompt,
                enableTrace=False,  # Set to True for debugging if needed
                endSession=False    # Keep session alive for follow-up questions
            )
               

        # Process the streaming response
        ai_response = ""
        event_stream = response['completion']
        
        try:
            for event in event_stream:
                if 'chunk' in event:
                    chunk = event['chunk']
                    if 'bytes' in chunk:
                        # Decode the bytes to string
                        chunk_text = chunk['bytes'].decode('utf-8')
                        ai_response += chunk_text
                elif 'trace' in event:
                    # Log trace information for debugging (if enableTrace=True)
                    trace = event['trace']
                    logger.debug(f"Agent trace for session {session_id}: {json.dumps(trace, default=str)}")
                elif 'returnControl' in event:
                    # Handle return control events if your agent uses function calling
                    return_control = event['returnControl']
                    logger.info(f"Agent return control for session {session_id}: {json.dumps(return_control, default=str)}")
                    
        except Exception as stream_error:
            logger.error(f"Error processing stream for session {session_id}: {str(stream_error)}")
            if ai_response.strip():
                # If we got partial response, use it
                logger.info(f"Using partial response for session {session_id}")
            else:
                return get_fallback_response()
                    
        if not ai_response.strip():
            logger.warning(f"Empty response from Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}")
            return get_fallback_response()
        
        logger.info(f"Successfully generated AI response from agent '{BEDROCK_AGENT_NAME}' for session {session_id}")
        return ai_response.strip()
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ThrottlingException':
            logger.warning(f"Bedrock Agent '{BEDROCK_AGENT_NAME}' throttling for session {session_id}")
        elif error_code == 'ValidationException':
            logger.error(f"Invalid request to Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}: {str(e)}")
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access denied to Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}")
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Bedrock Agent '{BEDROCK_AGENT_NAME}' or alias not found for session {session_id}: Agent ID {BEDROCK_AGENT_ID}, Alias ID {BEDROCK_AGENT_ALIAS_ID}")
        elif error_code == 'ServiceQuotaExceededException':
            logger.error(f"Service quota exceeded for Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}")
        elif error_code == 'ConflictException':
            logger.error(f"Conflict with Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}: {str(e)}")
        elif error_code == 'DependencyFailedException':
            logger.error(f"Dependency failure for Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}: {str(e)}")
        else:
            logger.error(f"Bedrock Agent '{BEDROCK_AGENT_NAME}' ClientError for session {session_id}: {error_code} - {str(e)}")
        return get_fallback_response()
        
    except Exception as e:
        logger.error(f"Unexpected error invoking Bedrock Agent '{BEDROCK_AGENT_NAME}' for session {session_id}: {str(e)}")
        return get_fallback_response()

def get_fallback_response():
    """
    Fallback response when Bedrock Agent is unavailable
    """
    return """I apologize, but I'm experiencing technical difficulties right now. However, I'm here to help with your technical career development!

Here are some ways I can assist you:

🚀 **Career Guidance**
• AI/ML career paths and opportunities
• Software engineering roles and progression
• Data science and analytics careers

💻 **Technical Skills**
• Programming languages to learn
• Frameworks and tools recommendations
• Best practices and coding standards

📚 **Learning Resources**
• Online courses and certifications
• Bootcamps and training programs
• Books and documentation

📝 **Professional Development**
• Resume and portfolio optimization
• Interview preparation strategies
• Networking and career growth tips

Please try asking your question again, or let me know what specific area you'd like to focus on!"""

def create_success_response(message, session_id, headers):
    """
    Create a successful response
    """
    return {
        'statusCode': 200,
        'headers': headers,
        'body': json.dumps({
            'response': message,
            'sessionId': session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'success',
            'agent': {
                'name': BEDROCK_AGENT_NAME,
                'id': BEDROCK_AGENT_ID,
                'aliasId': BEDROCK_AGENT_ALIAS_ID
            }
        })
    }

def create_error_response(error_message, headers, status_code=500):
    """
    Create an error response
    """
    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps({
            'error': error_message,
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'error',
            'agent': {
                'name': BEDROCK_AGENT_NAME,
                'id': BEDROCK_AGENT_ID,
                'aliasId': BEDROCK_AGENT_ALIAS_ID
            }
        })
    }
